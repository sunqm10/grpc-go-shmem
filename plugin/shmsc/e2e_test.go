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

package shmsc_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/benchmark"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	shmsc "google.golang.org/grpc/plugin/shmsc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestSelfContainedUnary exercises a full unary RPC over the self-contained SHM
// transport, selected end to end through the exported shmsc name resolver and
// pluggable-transport registries. Nothing here imports the engine or any
// internal package — the plugin is driven purely through public grpc-go APIs.
func TestSelfContainedUnary(t *testing.T) {
	name := fmt.Sprintf("shmsc_e2e_unary_%d", time.Now().UnixNano())

	lis, err := shmsc.Listen(name)
	if err != nil {
		t.Fatalf("shmsc.Listen: %v", err)
	}
	defer lis.Close()

	stopSrv := benchmark.StartServer(benchmark.ServerInfo{Type: "protobuf", Listener: lis})
	defer stopSrv()
	time.Sleep(100 * time.Millisecond)

	audiences := make(chan string, 1)
	conn, err := grpc.NewClient(shmsc.Name+":///"+name,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(authorityCaptureCreds{audiences: audiences}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := testpb.NewBenchmarkServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, sz := range []int{0, 1, 64, 4096, 65536} {
		req := &testpb.SimpleRequest{
			ResponseType: testpb.PayloadType_COMPRESSABLE,
			ResponseSize: int32(sz),
			Payload:      benchmark.NewPayload(testpb.PayloadType_COMPRESSABLE, sz),
		}
		resp, err := client.UnaryCall(ctx, req)
		if err != nil {
			t.Fatalf("UnaryCall(size=%d): %v", sz, err)
		}
		if got := len(resp.GetPayload().GetBody()); got != sz {
			t.Fatalf("UnaryCall(size=%d): response = %d bytes, want %d", sz, got, sz)
		}
		if sz == 0 {
			select {
			case got := <-audiences:
				if got != "https://localhost/grpc.testing.BenchmarkService" {
					t.Fatalf("credential audience = %q; want resolver authority localhost", got)
				}
			case <-ctx.Done():
				t.Fatal("timed out waiting for per-RPC credential audience")
			}
		}
	}
}

// TestResolverRetriesUntilListenerStarts verifies that name resolution is
// syntax-only: a missing segment fails in transport creation, enters normal
// ClientConn backoff, and succeeds after a listener for the same static name
// appears. The resolver must not probe segment existence in Build.
func TestResolverRetriesUntilListenerStarts(t *testing.T) {
	name := fmt.Sprintf("shmsc_late_listener_%d", time.Now().UnixNano())
	conn, err := grpc.NewClient(shmsc.Name+":///"+name,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	stateCtx, stateCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stateCancel()
	conn.Connect()
	if !waitForConnectivityState(stateCtx, conn, connectivity.TransientFailure) {
		t.Fatalf("channel state = %v; want TransientFailure before listener starts", conn.GetState())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := testpb.NewBenchmarkServiceClient(conn)
	callDone := make(chan error, 1)
	go func() {
		_, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.WaitForReady(true))
		callDone <- err
	}()
	select {
	case err := <-callDone:
		t.Fatalf("WaitForReady UnaryCall returned before listener started: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	lis, err := shmsc.Listen(name)
	if err != nil {
		t.Fatalf("shmsc.Listen: %v", err)
	}
	defer lis.Close()
	stopSrv := benchmark.StartServer(benchmark.ServerInfo{Type: "protobuf", Listener: lis})
	defer stopSrv()

	select {
	case err := <-callDone:
		if err != nil {
			t.Fatalf("UnaryCall after late listener start: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("WaitForReady UnaryCall did not complete after listener started")
	}
}

// TestResolverMissingSegmentFailsWithinDeadline verifies a permanently absent
// segment does not hang an RPC beyond its context deadline. The ClientConn may
// keep retrying with its normal backoff until it is closed.
func TestResolverMissingSegmentFailsWithinDeadline(t *testing.T) {
	name := fmt.Sprintf("shmsc_missing_%d", time.Now().UnixNano())
	conn, err := grpc.NewClient(shmsc.Name+":///"+name,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := testpb.NewBenchmarkServiceClient(conn)
	if _, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.WaitForReady(true)); err == nil {
		t.Fatal("UnaryCall to a missing segment succeeded; want a bounded failure")
	} else if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("UnaryCall error = %v; want DeadlineExceeded", err)
	}
}

func waitForConnectivityState(ctx context.Context, conn *grpc.ClientConn, want connectivity.State) bool {
	for state := conn.GetState(); state != want; state = conn.GetState() {
		if !conn.WaitForStateChange(ctx, state) {
			return false
		}
	}
	return true
}

type authorityCaptureCreds struct {
	audiences chan<- string
}

func (c authorityCaptureCreds) GetRequestMetadata(_ context.Context, uri ...string) (map[string]string, error) {
	if len(uri) > 0 {
		select {
		case c.audiences <- uri[0]:
		default:
		}
	}
	return map[string]string{}, nil
}

func (authorityCaptureCreds) RequireTransportSecurity() bool { return false }

// TestSelfContainedStreaming exercises bidirectional streaming ping-pong over
// the self-contained SHM transport selected through the registries.
func TestSelfContainedStreaming(t *testing.T) {
	name := fmt.Sprintf("shmsc_e2e_stream_%d", time.Now().UnixNano())

	lis, err := shmsc.Listen(name)
	if err != nil {
		t.Fatalf("shmsc.Listen: %v", err)
	}
	defer lis.Close()

	stopSrv := benchmark.StartServer(benchmark.ServerInfo{Type: "protobuf", Listener: lis})
	defer stopSrv()
	time.Sleep(100 * time.Millisecond)

	r := manual.NewBuilderWithScheme("shmscstream")
	r.InitialState(resolver.State{
		Addresses: []resolver.Address{{Addr: name, TransportType: shmsc.Name}},
	})

	conn, err := grpc.NewClient("shmscstream:///"+name,
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := testpb.NewBenchmarkServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream, err := client.StreamingCall(ctx)
	if err != nil {
		t.Fatalf("StreamingCall: %v", err)
	}

	const n = 8
	const sz = 1024
	for i := 0; i < n; i++ {
		req := &testpb.SimpleRequest{
			ResponseType: testpb.PayloadType_COMPRESSABLE,
			ResponseSize: sz,
			Payload:      benchmark.NewPayload(testpb.PayloadType_COMPRESSABLE, sz),
		}
		if err := stream.Send(req); err != nil {
			t.Fatalf("stream.Send #%d: %v", i, err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("stream.Recv #%d: %v", i, err)
		}
		if got := len(resp.GetPayload().GetBody()); got != sz {
			t.Fatalf("ping-pong #%d payload = %d bytes, want %d", i, got, sz)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
}

// TestRealTransportCredentialsRejected proves the security fail-closed posture
// end to end through the real grpc dial path: a channel configured with real
// (TLS) transport credentials over the shmsc transport must FAIL rather than be
// silently downgraded to an insecure shared-memory connection. No server is
// needed — the dial is refused by the plugin's builder before any connection.
func TestRealTransportCredentialsRejected(t *testing.T) {
	name := fmt.Sprintf("shmsc_tlsreject_%d", time.Now().UnixNano())
	r := manual.NewBuilderWithScheme("shmsctls")
	r.InitialState(resolver.State{
		Addresses: []resolver.Address{{Addr: name, TransportType: shmsc.Name}},
	})
	conn, err := grpc.NewClient("shmsctls:///"+name,
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient (lazy) unexpectedly failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = conn.Invoke(ctx, "/shmsc.test.NoSvc/NoMethod", &emptypb.Empty{}, &emptypb.Empty{})
	if err == nil {
		t.Fatal("RPC over a TLS-credentialed shmsc channel must fail fail-closed, but it succeeded")
	}
	if status.Code(err) == codes.OK {
		t.Fatalf("expected a non-OK failure, got OK")
	}
	t.Logf("TLS-credentialed shmsc dial correctly failed fail-closed: code=%v err=%v", status.Code(err), err)
}

// TestSelfContainedStatusDetails proves rich status (status.WithDetails)
// survives the round-trip over the self-contained SHM transport: the server
// serializes google.rpc.Status into grpc-status-details-bin and the client
// reconstructs it via status.FromProto.
func TestSelfContainedStatusDetails(t *testing.T) {
	name := fmt.Sprintf("shmsc_e2e_details_%d", time.Now().UnixNano())

	lis, err := shmsc.Listen(name)
	if err != nil {
		t.Fatalf("shmsc.Listen: %v", err)
	}
	defer lis.Close()

	wantDetail := &errdetails.ErrorInfo{Reason: "R", Domain: "D", Metadata: map[string]string{"k": "v"}}
	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "shmsc.test.DetailSvc",
		HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Do",
			Handler: func(_ interface{}, ctx context.Context, dec func(interface{}) error, _ grpc.UnaryServerInterceptor) (interface{}, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				st, derr := status.New(codes.FailedPrecondition, "boom").WithDetails(wantDetail)
				if derr != nil {
					return nil, derr
				}
				return nil, st.Err()
			},
		}},
	}, nil)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	time.Sleep(100 * time.Millisecond)

	r := manual.NewBuilderWithScheme("shmscdetails")
	r.InitialState(resolver.State{
		Addresses: []resolver.Address{{Addr: name, TransportType: shmsc.Name}},
	})
	conn, err := grpc.NewClient("shmscdetails:///"+name,
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = conn.Invoke(ctx, "/shmsc.test.DetailSvc/Do", &emptypb.Empty{}, &emptypb.Empty{})
	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition || st.Message() != "boom" {
		t.Fatalf("status = (%v, %q), want (FailedPrecondition, boom)", st.Code(), st.Message())
	}
	details := st.Details()
	if len(details) != 1 {
		t.Fatalf("got %d status details, want 1 (details lost in transit)", len(details))
	}
	ei, ok := details[0].(*errdetails.ErrorInfo)
	if !ok {
		t.Fatalf("detail type = %T, want *errdetails.ErrorInfo", details[0])
	}
	if ei.GetReason() != "R" || ei.GetDomain() != "D" || ei.GetMetadata()["k"] != "v" {
		t.Fatalf("detail content mismatch: %+v", ei)
	}
}
