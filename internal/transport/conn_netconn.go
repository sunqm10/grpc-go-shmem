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

package transport

import (
	"net"
	"time"
)

// This file completes the net.Conn interface for ShmConn (defined in
// conn.go), which already implements Read/Write/Close over the ring
// byte-pipe. These additions let ShmConn be handed to grpc-go's stock
// HTTP/2 transport via a Dialer/Listener — the "Track B" experiment
// where SHM provides a net.Conn instead of a full custom transport.
//
// Deadlines are accepted but ignored: the ring wait primitives block
// until data/space or connection close, not a wall-clock deadline. gRPC
// drives cancellation through stream context + transport Close(), so a
// missing deadline does not strand a connection in practice. A future
// revision may thread context-aware waits (ReadBlockingContext /
// WriteBlockingContext already exist) into a deadline implementation.

// shmPipeAddr is the net.Addr reported by a byte-pipe ShmConn. It names
// the underlying segment so channelz / logging show a stable identity.
type shmPipeAddr struct {
	name string
}

func (a shmPipeAddr) Network() string { return "shm" }
func (a shmPipeAddr) String() string  { return a.name }

// LocalAddr returns the local address (the segment name).
func (c *ShmConn) LocalAddr() net.Addr {
	return shmPipeAddr{name: c.segmentName}
}

// RemoteAddr returns the remote address (the segment name).
func (c *ShmConn) RemoteAddr() net.Addr {
	return shmPipeAddr{name: c.segmentName}
}

// SetDeadline is accepted but not enforced; see file-level comment.
func (c *ShmConn) SetDeadline(time.Time) error { return nil }

// SetReadDeadline is accepted but not enforced; see file-level comment.
func (c *ShmConn) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline is accepted but not enforced; see file-level comment.
func (c *ShmConn) SetWriteDeadline(time.Time) error { return nil }

// Compile-time assertion that ShmConn satisfies net.Conn.
var _ net.Conn = (*ShmConn)(nil)
