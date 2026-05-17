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
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// SHM in-process wake mechanism: an experimental override that
// substitutes Go channels for the cross-process futex syscall on the
// wake/signal hot path. ACTIVATED ONLY when the SHM_INPROC_WAKE=1
// env var is set, AND only when both endpoints are in the same Go
// process (the runtime can't tell at ring creation time, so the env
// var is the operator's promise).
//
// Why this exists: the kernel futex on Linux costs ~15–30 µs per
// wake/wait cycle through syscall.Syscall6's entersyscallblock /
// exitsyscall dance, mostly because exitsyscall triggers runtime.wakep
// to spread wake load across cores. UDS over the same Go runtime gets
// to ~3–5 µs per wake because Go's netpoll integration uses pure
// user-space gopark/goready instead of a real kernel syscall. So an
// SHM ping-pong RPC under no-spin pays ~85 µs more per round than UDS
// purely on this difference.
//
// In single-process deployments the cross-process safety of futex
// isn't needed. We register a process-global wake channel keyed by
// the shared *uint32 wake-address pointer; producer and consumer
// (different Ring structs wrapping the same shared memory) find each
// other via that key. Channels are gopark/goready under the hood —
// exactly the same primitive netpoll uses to make UDS fast.
//
// This is NOT a production replacement for the futex path. It's an
// experiment to validate the netpoll-class wake model. A production
// version would use eventfd + os.NewFile with the Go runtime's epoll
// registration for the cross-process case; same wake characteristics
// but with proper cross-process semantics.

// shmInprocWakeEnabled is true when the operator has promised that all
// endpoints of the SHM transport in this process are in the SAME
// process (no cross-process attaches). Set via SHM_INPROC_WAKE=1.
//
// Auto-disabled when GRPC_CROSS_PROCESS_CHILD=1 (the spawn marker used
// by TestCrossProcessEcho and similar tests): even if a developer leaves
// SHM_INPROC_WAKE=1 in the test environment, a forked child cannot see
// the parent's Go channels and would silently hang waiting on a wake
// that never comes. The marker check prevents that footgun.
//
// Real cross-process production deployments simply don't set
// SHM_INPROC_WAKE — they get the standard futex path which is
// cross-process safe.
var shmInprocWakeEnabled = os.Getenv("SHM_INPROC_WAKE") == "1" &&
	os.Getenv("GRPC_CROSS_PROCESS_CHILD") == ""

// shmInprocWaker is a Go-channel-backed waker keyed by a shared wake
// address pointer. The channel buffer is 1: wakes coalesce, and a
// wake when no one is parked is a no-op (the existing data-sequence
// re-check in the call site covers spurious / missed wakes).
type shmInprocWaker struct {
	c      chan struct{}
	closed atomic.Uint32
}

func newShmInprocWaker() *shmInprocWaker {
	return &shmInprocWaker{c: make(chan struct{}, 1)}
}

// Wake sends a non-blocking signal. Already-pending wakes are coalesced.
func (w *shmInprocWaker) Wake() {
	if w.closed.Load() != 0 {
		return
	}
	select {
	case w.c <- struct{}{}:
	default:
	}
}

// Wait blocks until Wake is called, context cancels, or timeout elapses.
func (w *shmInprocWaker) Wait(ctx context.Context, timeout time.Duration) error {
	if w.closed.Load() != 0 {
		return ErrRingClosed
	}
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		select {
		case <-w.c:
			return nil
		case <-t.C:
			return ErrFutexTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-w.c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shmInprocWakerRegistry maps (segmentID, byte-offset-within-mmap)
// to wake channels. Both the producer's signalData and the
// consumer's waitForData look up the same key because both reference
// the same shared-memory uint32 at the same offset within the same
// backing file.
//
// Why not key by *uint32 address directly: two ShmRings that wrap the
// same /dev/shm file via separate mmap calls get DIFFERENT virtual
// addresses (Linux mmap does not guarantee a specific vaddr; each
// mmap gets its own). So the producer's &hdr.dataSeq and the
// consumer's &hdr.dataSeq are not equal pointers — they alias the
// same byte but at different vaddrs. The byte offset within the
// segment IS identical across mappings, so that's our stable key.
var shmInprocWakerRegistry = struct {
	mu     sync.Mutex
	wakers map[shmInprocKey]*shmInprocWaker
}{wakers: make(map[shmInprocKey]*shmInprocWaker)}

type shmInprocKey struct {
	segmentID string
	offset    uintptr // offset of the wake-address uint32 within the mmap
}

// getInprocWaker returns the waker for the given (segmentID, addr)
// pair, creating one on first call. base is the mmap base pointer
// (i.e., &r.mem[0]); addr is the wake-address uint32 within that
// mmap. Their difference gives the stable offset key.
func getInprocWaker(segmentID string, base unsafe.Pointer, addr *uint32) *shmInprocWaker {
	key := shmInprocKey{
		segmentID: segmentID,
		offset:    uintptr(unsafe.Pointer(addr)) - uintptr(base),
	}
	shmInprocWakerRegistry.mu.Lock()
	w, ok := shmInprocWakerRegistry.wakers[key]
	if !ok {
		w = newShmInprocWaker()
		shmInprocWakerRegistry.wakers[key] = w
	}
	shmInprocWakerRegistry.mu.Unlock()
	return w
}

// dropInprocWakersForSegment removes every waker tied to the given
// segmentID. Called when a segment closes so the registry doesn't
// accumulate dead entries across tests / connections, and so any
// goroutines still parked on a stale waker get released (Wake()'d)
// and exit through the ctx-done branch of their Wait loop.
//
// Safe to call multiple times; idempotent.
func dropInprocWakersForSegment(segmentID string) {
	if segmentID == "" {
		return
	}
	shmInprocWakerRegistry.mu.Lock()
	defer shmInprocWakerRegistry.mu.Unlock()
	for k, w := range shmInprocWakerRegistry.wakers {
		if k.segmentID != segmentID {
			continue
		}
		// Mark closed, then drain the channel so any subsequent
		// Wait calls fall straight through (the existing call sites
		// always re-check the underlying shared-memory state after
		// a wake return).
		w.closed.Store(1)
		select {
		case w.c <- struct{}{}:
		default:
		}
		delete(shmInprocWakerRegistry.wakers, k)
	}
}
