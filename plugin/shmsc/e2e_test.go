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
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/benchmark"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	shmsc "google.golang.org/grpc/plugin/shmsc"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// TestSelfContainedUnary exercises a full unary RPC over the self-contained SHM
// transport, selected end to end through the exported D1 pluggable-transport
// registries: the client via resolver.Address.TransportType == shmsc.Name and
// the server via the tagged listener. Nothing here imports the engine or any
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

	r := manual.NewBuilderWithScheme("shmscunary")
	r.InitialState(resolver.State{
		Addresses: []resolver.Address{{Addr: name, TransportType: shmsc.Name}},
	})

	conn, err := grpc.NewClient("shmscunary:///"+name,
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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
	}
}

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
