//go:build linux || windows

/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Track B write-probe: empirical evidence for why a write-side zero-copy
// (ZC) optimisation cannot fire under the stock grpc-go framer when SHM is
// plugged in only as a net.Conn.
//
// The design analysis (Opus §5) proposed reclaiming the lost write-side
// copy with a ring-backed mem.BufferPool: have the codec marshal the proto
// directly into the send ring, then an "alias-detecting" ShmConn.Write
// would notice the buffer already lives in the ring and skip the copy.
//
// That trick can only fire if the codec's marshal buffer lands at the
// ring's *current write head* and stays contiguous through to conn.Write.
// But a ring is sequential, and grpc-go's framer interleaves variable
// length HTTP/2 / HPACK header bytes *between* the codec's pool.Get (when
// the payload buffer is allocated) and the framer's conn.Write of that
// payload. The header bytes are written to the ring first, advancing the
// head past wherever the payload buffer would have been reserved. So the
// payload can never be placed contiguously at the head by a buffer pool
// that runs at marshal time — the same structural reason read-side ZC is
// blocked (the framer picks the destination before the bytes move).
//
// This probe makes that interleaving directly observable: it wraps each
// ShmConn in a counter that records the size distribution of every
// conn.Write the framer issues. The histogram shows (a) payloads larger
// than the HTTP/2 frame cap arrive as a run of ~16 KiB writes, and
// (b) small header writes are interleaved with them — both of which a
// ring-backed pool cannot collapse into one contiguous ZC reservation.
//
// Run:
//
//	go test -run TestTrackBWriteProbe -v ./benchmark/shmemtcp
//
// It is a Test (not a Benchmark) because we only need a fixed, small
// number of RPCs to capture the steady-state write pattern; ns/op is
// irrelevant here.

package main

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/benchmark"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

// writeHist accumulates the size distribution of conn.Write calls on one
// direction of a Track B connection. Buckets are keyed by an upper power
// of two so the output stays compact.
type writeHist struct {
	mu      sync.Mutex
	calls   int64
	bytes   int64
	buckets map[int]int64 // bucket upper bound -> call count
	maxSize int
}

func newWriteHist() *writeHist {
	return &writeHist{buckets: make(map[int]int64)}
}

// bucketFor returns a stable, human-readable upper bound for n. The
// boundaries straddle the 16 KiB HTTP/2 frame cap so the chunking is
// obvious in the output.
func bucketFor(n int) int {
	switch {
	case n <= 16:
		return 16
	case n <= 64:
		return 64
	case n <= 256:
		return 256
	case n <= 1024:
		return 1024
	case n <= 4096:
		return 4096
	case n <= 16384:
		return 16384
	case n <= 65536:
		return 65536
	default:
		return 1 << 30 // ">64KiB"
	}
}

func (h *writeHist) record(n int) {
	h.mu.Lock()
	h.calls++
	h.bytes += int64(n)
	h.buckets[bucketFor(n)]++
	if n > h.maxSize {
		h.maxSize = n
	}
	h.mu.Unlock()
}

func (h *writeHist) dump(t *testing.T, label string, rpcs int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	bounds := make([]int, 0, len(h.buckets))
	for b := range h.buckets {
		bounds = append(bounds, b)
	}
	sort.Ints(bounds)
	t.Logf("[%s] %d RPCs: %d conn.Write calls, %d bytes (%.1f writes/RPC, largest single Write=%d B)",
		label, rpcs, h.calls, h.bytes, float64(h.calls)/float64(rpcs), h.maxSize)
	for _, b := range bounds {
		name := fmt.Sprintf("<=%d", b)
		if b == 1<<30 {
			name = ">64KiB"
		}
		t.Logf("    Write size %-8s : %d calls", name, h.buckets[b])
	}
}

// probeConn wraps a net.Conn and records every Write size into hist.
type probeConn struct {
	net.Conn
	hist *writeHist
}

func (c *probeConn) Write(p []byte) (int, error) {
	c.hist.record(len(p))
	return c.Conn.Write(p)
}

// TestTrackBWriteProbe drives a fixed number of streaming RPCs through the
// Track B (net.Conn-over-SHM) path with ZeroBuf (WithWriteBufferSize(0)) so
// every framer write reaches conn.Write directly, and prints the write-size
// histogram for the client->server direction. This is the empirical
// evidence that a ring-backed write-ZC pool cannot fire: the payload is
// split at the 16 KiB frame cap and interleaved with header writes, so no
// single contiguous ring reservation can hold a >16 KiB message.
func TestTrackBWriteProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("write-probe drives real RPCs; skipped in -short")
	}

	const rpcs = 200
	sizes := []int{1024, 64 << 10, 256 << 10}

	for _, size := range sizes {
		size := size
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			clientHist := newWriteHist()
			wrap := func(c net.Conn, isServer bool) net.Conn {
				if isServer {
					return c // only instrument the client->server send path
				}
				return &probeConn{Conn: c, hist: clientHist}
			}

			// ZeroBuf so framer writes hit conn.Write directly (no bufio
			// staging that would coalesce/obscure the per-frame writes).
			env := newTrackBEnvWrap(t, 0, 0, wrap)
			defer env.close()

			req := &testpb.SimpleRequest{
				ResponseType: testpb.PayloadType_COMPRESSABLE,
				ResponseSize: int32(size),
				Payload:      benchmark.NewPayload(testpb.PayloadType_COMPRESSABLE, size),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			stream, err := env.client.StreamingCall(ctx)
			if err != nil {
				t.Fatalf("StreamingCall: %v", err)
			}
			// Warm up one round trip outside the measured window so the
			// initial HEADERS/SETTINGS frames don't skew the histogram.
			if err := stream.Send(req); err != nil {
				t.Fatalf("warm-up Send: %v", err)
			}
			if _, err := stream.Recv(); err != nil {
				t.Fatalf("warm-up Recv: %v", err)
			}

			// Reset the histogram so we only count steady-state DATA frames.
			clientHist.mu.Lock()
			clientHist.calls = 0
			clientHist.bytes = 0
			clientHist.maxSize = 0
			clientHist.buckets = make(map[int]int64)
			clientHist.mu.Unlock()

			for i := 0; i < rpcs; i++ {
				if err := stream.Send(req); err != nil {
					t.Fatalf("Send: %v", err)
				}
				if _, err := stream.Recv(); err != nil {
					t.Fatalf("Recv: %v", err)
				}
			}
			_ = stream.CloseSend()
			for {
				if _, err := stream.Recv(); err != nil {
					break
				}
			}

			clientHist.dump(t, fmt.Sprintf("client->server size=%d", size), rpcs)

			// The actionable finding for the doc: writes/RPC > 1 (header +
			// chunked payload) means a ring-backed write-ZC pool cannot
			// place the message as one contiguous reservation.
			clientHist.mu.Lock()
			writesPerRPC := float64(clientHist.calls) / float64(rpcs)
			maxSize := clientHist.maxSize
			clientHist.mu.Unlock()
			if size > 16<<10 && maxSize > 16<<10 {
				t.Errorf("unexpected: a single Write carried %d B (> 16 KiB frame cap); "+
					"frame chunking assumption needs revisiting", maxSize)
			}
			t.Logf("[%s size=%d] writes/RPC=%.2f -> write-ZC pool would need %.0f contiguous ring reservations per message",
				"client->server", size, writesPerRPC, writesPerRPC)
		})
	}
}
