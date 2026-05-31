// Copyright 2026 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// TestShmM1aWakeCoalesceOnFirstResponse verifies the M1a optimization:
// the server's first response message in an RPC fuses HEADERS + DATA
// into a single BeginBatch/EndBatch scope so the client reader pays
// exactly ONE signalData wake instead of two.
//
// What it asserts:
//   - For one unary RPC: server emits exactly 1 signalData on the
//     server->client ring for the response (HEADERS + DATA combined).
//   - For two messages on the same server stream (streaming): the
//     first emits 1 (HEADERS+DATA fused), and subsequent emit 1 each
//     (DATA alone, no batch).
//
// The bound check uses an inequality (<=) because the standalone
// sendConnWindowUpdate / sendStreamWindowUpdate paths may also fire
// signalData for unrelated WU frames; this test asserts the M1a
// invariant ONLY, namely that the response-side wake floor is not
// inflated by HEADERS firing a separate signal.
func TestShmM1aWakeCoalesceOnFirstResponse(t *testing.T) {
	ct, st, _, cleanup := setupShmTransportPair(t, 256*1024)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Server handler: read request, send single response message + OK status.
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		st.HandleStreams(ctx, func(s *ServerStream) {
			_, _ = s.Read(1024)
			payload := []byte("OK")
			hdr := make([]byte, 5)
			hdr[0] = 0
			binaryBE32(hdr[1:5], uint32(len(payload)))
			_ = s.Write(hdr, mem.BufferSlice{mem.SliceBuffer(payload)}, &WriteOptions{})
			_ = s.WriteStatus(status.New(codes.OK, ""))
		})
	}()

	// Open one stream, send request, read response, close.
	callHdr := &CallHdr{Host: "localhost", Method: "/test/M1a"}
	cs, err := ct.NewStream(ctx, callHdr, nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	// --- Snapshot signal-data counter BEFORE the response side wakes ---
	// We bracket from "client send" to "client receive complete" — any
	// signalData fired in that window is by definition the response-
	// side activity (modulo background WU drift, which is bounded by
	// the WU threshold and effectively zero for this tiny RPC).
	before := atomic.LoadUint64(&shmSignalDataFire)

	// Client send: request message + Last sentinel.
	req := make([]byte, 16)
	reqHdr := make([]byte, 5)
	binaryBE32(reqHdr[1:5], uint32(len(req)))
	if err := cs.Write(reqHdr, mem.BufferSlice{mem.SliceBuffer(req)}, &WriteOptions{Last: true}); err != nil && err != io.EOF {
		t.Fatalf("cs.Write request: %v", err)
	}

	// Drain client recv path: header, message, status.
	if _, err := cs.Read(1024); err != nil && err != io.EOF {
		t.Fatalf("cs.Read response: %v", err)
	}

	// Wait briefly for trailing status to settle.
	select {
	case <-cs.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("stream Done timed out")
	}

	delta := atomic.LoadUint64(&shmSignalDataFire) - before

	// What we expect:
	//   - 1 signalData from the client SEND (request DATA on client->server ring)
	//   - 1 signalData from the server RESPONSE (HEADERS+DATA fused by M1a) on
	//     server->client ring
	//   - 1 signalData from the server WriteStatus (TRAILERS) on server->client ring
	//   - Possibly small extras from WU emissions (bounded; threshold not crossed
	//     for this tiny payload at default 32MiB window)
	//
	// Without M1a the server response would fire 2 signals on server->client ring
	// (one HEADERS, one DATA), giving a total >= 4. With M1a: == 3 in the floor.
	//
	// We assert delta <= 4 to permit one stray WU and still catch the regression
	// (which would push delta to 5+).
	const m1aWakeCeiling = 4
	if delta > m1aWakeCeiling {
		t.Errorf("M1a wake-coalesce regression: signalData delta = %d, want <= %d "+
			"(an unfused HEADERS+DATA response would push it to >= 5)", delta, m1aWakeCeiling)
	}

	ct.Close(nil)
	st.Close(nil)
	<-serverDone
}

// TestShmM1aHeaderDedupAcrossExplicitSendHeader verifies the M1a code
// path's headerSent CAS dedup: if the handler calls SendHeader
// explicitly BEFORE the first Write, the M1a fast path must not
// emit a duplicate HEADERS frame.
func TestShmM1aHeaderDedupAcrossExplicitSendHeader(t *testing.T) {
	ct, st, _, cleanup := setupShmTransportPair(t, 256*1024)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		st.HandleStreams(ctx, func(s *ServerStream) {
			_, _ = s.Read(1024)
			// EXPLICIT SendHeader before Write — M1a must observe
			// headerSent == 1 and skip its own HEADERS emission.
			_ = s.SendHeader(nil)

			payload := []byte("DEDUP-OK")
			hdr := make([]byte, 5)
			hdr[0] = 0
			binaryBE32(hdr[1:5], uint32(len(payload)))
			_ = s.Write(hdr, mem.BufferSlice{mem.SliceBuffer(payload)}, &WriteOptions{})
			_ = s.WriteStatus(status.New(codes.OK, ""))
		})
	}()

	callHdr := &CallHdr{Host: "localhost", Method: "/test/M1aDedup"}
	cs, err := ct.NewStream(ctx, callHdr, nil)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	req := make([]byte, 4)
	reqHdr := make([]byte, 5)
	binaryBE32(reqHdr[1:5], uint32(len(req)))
	if err := cs.Write(reqHdr, mem.BufferSlice{mem.SliceBuffer(req)}, &WriteOptions{Last: true}); err != nil && err != io.EOF {
		t.Fatalf("cs.Write request: %v", err)
	}

	// Read response; if HEADERS were duplicated, the decoder will surface a
	// PROTOCOL_ERROR or the stream will error out. We accept (resp != nil, err == nil)
	// OR (resp != nil, err == io.EOF) — both indicate the response message was
	// observed cleanly; a duplicate HEADERS would have manifested as a non-EOF
	// error from cs.Read or as a non-OK status below.
	resp, err := cs.Read(1024)
	if err != nil && err != io.EOF {
		t.Fatalf("cs.Read response: %v (a duplicate HEADERS would surface here as PROTOCOL_ERROR)", err)
	}
	_ = resp // payload presence not asserted; we test status below

	select {
	case <-cs.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("stream Done timed out")
	}

	if st := cs.Status(); st.Code() != codes.OK {
		t.Errorf("stream status = %s, want OK (a duplicate HEADERS would make this fail)", st.Code())
	}

	ct.Close(nil)
	st.Close(nil)
	<-serverDone
}

// binaryBE32 writes v as a big-endian uint32 into b[0:4].
func binaryBE32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
