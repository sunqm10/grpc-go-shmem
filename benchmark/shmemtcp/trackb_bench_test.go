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

// Track B ("net.Conn-over-SHM") benchmarks.
//
// These exercise stock gRPC HTTP/2 over a shared-memory byte pipe handed
// to grpc-go as a plain net.Conn — the "Dialer/Listener only, zero core
// changes" shape Doug Fawley asked about. Unlike the full SHM transport
// (BenchmarkGRPCShm*), this path keeps grpc-go's framer / loopyWriter /
// HPACK / codec entirely intact and only replaces the AF_UNIX socket with
// ring + wake. It is the empirical floor for "how much of the SHM win
// survives the net.Conn boundary".
//
// What this measures vs the full transport:
//   - PRESERVED: kernel bypass (no socket syscalls), eventfd/futex wake.
//   - LOST: user-space transport-internal zero-copy (grpc-go copies into
//     its framer buffer, so ShmConn.Read/Write must copy ring<->buffer).
//
// The client and server run in one process (like the UDS/TCP benches), so
// the connection handshake uses an in-process rendezvous. That is a
// one-time setup cost outside the timing loop; steady-state per-RPC cost
// through the ring is identical to a cross-process handshake.
//
// NOTE on wake primitive: this floor harness uses the ring's default
// wait/signal (futex on Linux) rather than the per-segment eventfd waker
// the full SHM bench enables. The copy question this harness answers is
// independent of the wake primitive; a follow-up can wire eventfd for a
// fully apples-to-apples wake comparison.

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/benchmark"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal/transport"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
)

// trackBRing is the per-direction ring capacity for Track B segments.
// Smaller than the full-transport benchRing (64 MiB) because stock
// grpc-go caps DATA frames at ~16 KiB, so the ring never needs to hold
// a large contiguous frame; it only buffers in-flight framed bytes.
const trackBRing = 8 * 1024 * 1024 // 8 MiB

// trackBReq is a single connection request handed from the dialer to the
// listener's Accept loop.
type trackBReq struct {
	name  string
	ready chan error // listener sends nil once the segment is created + ready
}

// trackBListener is a minimal net.Listener whose Accept returns
// server-side ShmConn byte pipes. It rendezvous with the in-process
// dialer over reqCh.
type trackBListener struct {
	reqCh  chan *trackBReq
	addr   net.Addr
	closed chan struct{}
	// wrap, when non-nil, decorates every accepted/dialed ShmConn before
	// it is handed to grpc-go. isServer distinguishes the two ends. The
	// floor harness leaves this nil (no behaviour change); the write-probe
	// variant uses it to count Write sizes at the ring boundary.
	wrap func(c net.Conn, isServer bool) net.Conn
}

func newTrackBListener(name string) *trackBListener {
	return &trackBListener{
		reqCh:  make(chan *trackBReq),
		addr:   &transport.ShmAddr{Name: name},
		closed: make(chan struct{}),
	}
}

// Accept waits for a dial request, creates the segment (server owns it,
// matching production), and returns the server-side byte pipe.
func (l *trackBListener) Accept() (net.Conn, error) {
	select {
	case req := <-l.reqCh:
		seg, err := transport.CreateSegment(req.name, trackBRing, trackBRing)
		if err != nil {
			req.ready <- err
			return nil, err
		}
		seg.H.SetServerReady(true)
		var conn net.Conn = transport.NewServerConn(seg) // registers rings before client opens
		if l.wrap != nil {
			conn = l.wrap(conn, true)
		}
		req.ready <- nil
		return conn, nil
	case <-l.closed:
		return nil, errors.New("trackB listener closed")
	}
}

func (l *trackBListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (l *trackBListener) Addr() net.Addr { return l.addr }

// dial is the client side of the rendezvous: request a fresh segment,
// wait for the listener to create it, then open it as the client.
func (l *trackBListener) dial(ctx context.Context) (net.Conn, error) {
	name := fmt.Sprintf("grpc_shm_trackb_%d", time.Now().UnixNano())
	req := &trackBReq{name: name, ready: make(chan error, 1)}

	select {
	case l.reqCh <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.closed:
		return nil, errors.New("trackB listener closed")
	}

	select {
	case err := <-req.ready:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	seg, err := transport.OpenSegment(name)
	if err != nil {
		return nil, err
	}
	var conn net.Conn = transport.NewClientConn(seg)
	if l.wrap != nil {
		conn = l.wrap(conn, false)
	}
	return conn, nil
}

// newTrackBEnv builds a stock gRPC server+client pair whose only
// non-standard piece is the net.Conn (ShmConn byte pipe). writeBuf /
// readBuf < 0 means "use grpc-go defaults"; 0 means disable the staging
// buffer (the cheap-win configuration from the design analysis).
func newTrackBEnv(b *testing.B, writeBuf, readBuf int) *grpcBenchEnv {
	return newTrackBEnvWrap(b, writeBuf, readBuf, nil)
}

// newTrackBEnvWrap is newTrackBEnv with an optional ShmConn decorator
// (used by the write-probe variant to instrument the ring boundary).
func newTrackBEnvWrap(b testing.TB, writeBuf, readBuf int, wrap func(c net.Conn, isServer bool) net.Conn) *grpcBenchEnv {
	logBenchEnvOnce(b)
	name := fmt.Sprintf("grpc_shm_trackb_ctl_%d", time.Now().UnixNano())
	lis := newTrackBListener(name)
	lis.wrap = wrap

	srvOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(benchMaxMsg),
		grpc.MaxSendMsgSize(benchMaxMsg),
	}
	if writeBuf >= 0 {
		srvOpts = append(srvOpts, grpc.WriteBufferSize(writeBuf))
	}
	if readBuf >= 0 {
		srvOpts = append(srvOpts, grpc.ReadBufferSize(readBuf))
	}
	stop := benchmark.StartServer(benchmark.ServerInfo{Type: "protobuf", Listener: lis}, srvOpts...)

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.dial(ctx)
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(benchMaxMsg),
			grpc.MaxCallSendMsgSize(benchMaxMsg),
		),
	}
	if writeBuf >= 0 {
		dialOpts = append(dialOpts, grpc.WithWriteBufferSize(writeBuf))
	}
	if readBuf >= 0 {
		dialOpts = append(dialOpts, grpc.WithReadBufferSize(readBuf))
	}
	conn, err := grpc.NewClient("passthrough:///trackb", dialOpts...)
	if err != nil {
		stop()
		lis.Close()
		b.Fatalf("NewClient: %v", err)
	}

	client := testgrpc.NewBenchmarkServiceClient(conn)
	warmUpGRPC(b, client)

	return &grpcBenchEnv{
		stopSrv: stop,
		conn:    conn,
		client:  client,
		cleanups: []func(){
			func() { lis.Close() },
		},
	}
}

// BenchmarkGRPCTrackBStream measures streaming ping-pong over net.Conn-over-SHM
// with grpc-go default read/write buffers (the pessimistic floor).
func BenchmarkGRPCTrackBStream(b *testing.B) {
	env := newTrackBEnv(b, -1, -1)
	defer env.close()
	for _, p := range benchPayloadSizes {
		p := p
		b.Run(fmt.Sprintf("size=%s", p.label), func(b *testing.B) {
			benchStream(b, env.client, p.bytes)
		})
	}
}

// BenchmarkGRPCTrackBStreamZeroBuf is the same as BenchmarkGRPCTrackBStream
// but disables grpc-go's read/write staging buffers (WithWriteBufferSize(0)
// + WithReadBufferSize(0)) — the "cheap win" that removes the extra
// staging copy, leaving exactly one ring<->framer copy per direction.
func BenchmarkGRPCTrackBStreamZeroBuf(b *testing.B) {
	env := newTrackBEnv(b, 0, 0)
	defer env.close()
	for _, p := range benchPayloadSizes {
		p := p
		b.Run(fmt.Sprintf("size=%s", p.label), func(b *testing.B) {
			benchStream(b, env.client, p.bytes)
		})
	}
}

// BenchmarkGRPCTrackBUnary measures unary RPC over net.Conn-over-SHM with
// grpc-go default buffers.
func BenchmarkGRPCTrackBUnary(b *testing.B) {
	env := newTrackBEnv(b, -1, -1)
	defer env.close()
	for _, p := range benchPayloadSizes {
		p := p
		b.Run(fmt.Sprintf("size=%s", p.label), func(b *testing.B) {
			benchUnary(b, env.client, p.bytes)
		})
	}
}

// BenchmarkGRPCTrackBUnaryZeroBuf is BenchmarkGRPCTrackBUnary with the
// staging buffers disabled.
func BenchmarkGRPCTrackBUnaryZeroBuf(b *testing.B) {
	env := newTrackBEnv(b, 0, 0)
	defer env.close()
	for _, p := range benchPayloadSizes {
		p := p
		b.Run(fmt.Sprintf("size=%s", p.label), func(b *testing.B) {
			benchUnary(b, env.client, p.bytes)
		})
	}
}
