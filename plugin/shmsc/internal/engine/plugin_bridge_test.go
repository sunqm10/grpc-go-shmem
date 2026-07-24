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

package engine

import (
	"context"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc/credentials"
	client "google.golang.org/grpc/experimental/transport/client"
	server "google.golang.org/grpc/experimental/transport/server"
)

// fakeTC is a minimal credentials.TransportCredentials reporting a configurable
// security protocol, used to prove the fail-closed rejection paths without a
// real TLS setup.
type fakeTC struct{ proto string }

func (f fakeTC) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, nil
}
func (f fakeTC) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, nil
}
func (f fakeTC) Info() credentials.ProtocolInfo         { return credentials.ProtocolInfo{SecurityProtocol: f.proto} }
func (f fakeTC) Clone() credentials.TransportCredentials { return f }
func (f fakeTC) OverrideServerName(string) error        { return nil }

// fakePerRPC is a minimal per-RPC credential that requires transport security.
type fakePerRPC struct{ requireSec bool }

func (f fakePerRPC) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "bearer x"}, nil
}
func (f fakePerRPC) RequireTransportSecurity() bool { return f.requireSec }

func TestDialClientRejectsRealTransportCredentials(t *testing.T) {
	_, err := DialClient(context.Background(), "unused", client.BuildOptions{
		TransportCredentials: fakeTC{proto: "tls"},
	})
	if err == nil || !strings.Contains(err.Error(), "transport security") {
		t.Fatalf("expected fail-closed rejection of TLS credentials, got err=%v", err)
	}
}

func TestDialClientAcceptsInsecureCredentials(t *testing.T) {
	// An insecure credential must NOT be rejected by the security gate. It will
	// fail later in DialShm (no server segment named "unused"), which is fine —
	// we only assert the error is NOT the security rejection.
	_, err := DialClient(context.Background(), "unused_no_server", client.BuildOptions{
		TransportCredentials: fakeTC{proto: "insecure"},
	})
	if err != nil && strings.Contains(err.Error(), "transport security") {
		t.Fatalf("insecure credentials must pass the security gate, got %v", err)
	}
}

func TestDialClientRejectsRequireTransportSecurityPerRPC(t *testing.T) {
	_, err := DialClient(context.Background(), "unused", client.BuildOptions{
		PerRPCCredentials: []credentials.PerRPCCredentials{fakePerRPC{requireSec: true}},
	})
	if err == nil || !strings.Contains(err.Error(), "per-RPC credential") {
		t.Fatalf("expected fail-closed rejection of RequireTransportSecurity per-RPC creds, got err=%v", err)
	}
}

func TestBuildServerRejectsRealCredentials(t *testing.T) {
	_, err := BuildServer(nil, server.BuildOptions{Credentials: fakeTC{proto: "tls"}})
	if err == nil || !strings.Contains(err.Error(), "transport security") {
		t.Fatalf("expected fail-closed rejection of real server credentials, got err=%v", err)
	}
}
