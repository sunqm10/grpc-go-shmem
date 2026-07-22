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
	"fmt"
	"net"
	"time"

	client "google.golang.org/grpc/experimental/transport/client"
	server "google.golang.org/grpc/experimental/transport/server"
)

// DialClient dials a shared-memory segment and returns the D1 client transport,
// wiring the public D1 client BuildOptions into the engine's DialOptions. It is
// the single entry point the self-contained plugin's client Builder calls, so
// the unexported engine transport type never has to be named outside this
// package.
func DialClient(connectCtx context.Context, addr string, opts client.BuildOptions) (client.ClientTransport, error) {
	dopts := DefaultDialOptions()
	if dl, ok := connectCtx.Deadline(); ok {
		if d := time.Until(dl); d > 0 {
			dopts.ConnectTimeout = d
		}
	}
	dopts.KeepaliveParams = opts.Keepalive
	// Flow-control windows: apply only when in the valid HTTP/2 range so an
	// out-of-range uint32 cannot wrap negative on the int32 conversion.
	if opts.InitialWindowSize >= defaultWindowSize && opts.InitialWindowSize <= maxWindowSize {
		dopts.InitialWindowSize = int32(opts.InitialWindowSize)
	}
	if opts.InitialConnWindowSize > 0 && opts.InitialConnWindowSize <= maxWindowSize {
		dopts.InitialConnWindowSize = int32(opts.InitialConnWindowSize)
	}
	// A non-nil TransportCredentials selects the SHM security handshake. The
	// handshake installs the ShmAuthInfo the client transport reports via
	// SecurityInfo/Peer. A nil credential leaves the connection insecure.
	if opts.TransportCredentials != nil {
		dopts.Handshaker = DefaultShmHandshaker()
	}
	t, err := DialShm(connectCtx, addr, dopts)
	if err != nil {
		return nil, err
	}
	if opts.OnClose != nil {
		oc := opts.OnClose
		t.SetOnClose(func(gi GoAwayInfo) { oc(client.CloseInfo{Err: gi.Err}) })
	}
	return t, nil
}

// BuildServer extracts the server transport an accepted shared-memory
// connection carries and applies the D1 server BuildOptions before it begins
// serving. It is the entry point the plugin's server Builder calls.
func BuildServer(conn net.Conn, opts server.BuildOptions) (server.ServerTransport, error) {
	sc := asShmConn(conn)
	if sc == nil {
		return nil, fmt.Errorf("shmsc: connection %T is not a shared-memory connection", conn)
	}
	t := sc.GetServerTransport()
	if t == nil {
		return nil, fmt.Errorf("shmsc: shared-memory connection has no server transport")
	}
	t.ApplyServerBuildOptions(opts)
	return t, nil
}

// asShmConn returns the underlying *shmConn for conn, unwrapping any
// transport-type-tagging wrappers the plugin's listener layered on top. It
// bounds the unwrap depth to avoid a pathological cyclic wrapper.
func asShmConn(conn net.Conn) *shmConn {
	for i := 0; conn != nil && i < 8; i++ {
		if sc, ok := conn.(*shmConn); ok {
			return sc
		}
		u, ok := conn.(interface{ Unwrap() net.Conn })
		if !ok {
			return nil
		}
		conn = u.Unwrap()
	}
	return nil
}
