//go:build darwin

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

// Darwin twin of handshake_linux.go over the polling futex primitives.
//
// One deliberate difference from Linux: waits are CHUNKED (<= 50ms per
// futexWaitTimeout call) instead of passing the entire remaining deadline in
// one shot. On Linux a context cancellation without a deadline is only
// noticed when the peer's wake arrives; with the polling primitives there is
// no wake at all, so the loop must resurface periodically to observe
// ctx.Done(). The chunk expiry is not a deadline: the loop re-checks the
// flag and the context, then waits again.

package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
	"unsafe"
)

// handshakePollChunk bounds how long one wait runs before re-checking ctx.
const handshakePollChunk = 50 * time.Millisecond

// WaitForClient waits for the client to mark itself as ready.
func (s *Segment) WaitForClient(ctx context.Context) error {
	addr := (*uint32)(unsafe.Pointer(&s.H.header().clientReady))
	return waitForReadyFlag(ctx, addr)
}

// WaitForServer waits for the server to mark itself as ready.
func (s *Segment) WaitForServer(ctx context.Context) error {
	addr := (*uint32)(unsafe.Pointer(&s.H.header().serverReady))
	return waitForReadyFlag(ctx, addr)
}

func waitForReadyFlag(ctx context.Context, addr *uint32) error {
	// Fast path.
	if atomic.LoadUint32(addr) != 0 {
		return nil
	}
	for {
		if atomic.LoadUint32(addr) != 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Wait in bounded chunks. When a deadline exists, the final chunk
		// is trimmed to it so DeadlineExceeded is reported on time.
		chunk := handshakePollChunk
		deadlined := false
		if dl, ok := ctx.Deadline(); ok {
			remaining := time.Until(dl)
			if remaining <= 0 {
				return context.DeadlineExceeded
			}
			if remaining <= chunk {
				chunk = remaining
				deadlined = true
			}
		}
		if err := futexWaitTimeout(addr, 0, chunk.Nanoseconds()); err != nil {
			if errors.Is(err, ErrFutexTimeout) {
				// Re-check the flag first to catch a peer store landing on
				// the timeout boundary (same rationale as Linux). A chunk
				// expiry that was not the real deadline just loops.
				if atomic.LoadUint32(addr) != 0 {
					return nil
				}
				if deadlined {
					return context.DeadlineExceeded
				}
				continue
			}
			return err
		}
	}
}
