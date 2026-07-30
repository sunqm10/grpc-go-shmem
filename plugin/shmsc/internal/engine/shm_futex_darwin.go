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

// Darwin twin of the Linux futex primitives, implemented as an adaptive
// poll on the shared word.
//
// macOS has no cross-process futex reachable from pure Go: the public
// os_sync_wait_on_address API is libSystem-only (and macOS 14.4+), and the
// __ulock_wait/__ulock_wake syscalls are private XNU ABI. Rather than take a
// cgo or private-ABI dependency, the darwin port polls: waiters sleep in
// escalating slices between atomic re-checks of the word, and wakes are
// no-ops.
//
// This is CORRECT under the engine's protocol because a futex wake is
// advisory there — every waiter re-validates its condition (ring indices,
// sequence words, ready flags) after any return from a wait, and both
// futexWait and futexWaitTimeout are allowed to return spuriously (the
// Linux implementation already returns nil on EAGAIN/EINTR). The cost is
// latency, bounded by the backoff ceiling below, and a small idle cost
// (parked goroutines wake once per ceiling interval to re-check).
//
// Backoff shape: brief Gosched yields (covers back-to-back traffic where
// the word flips within microseconds), then sleeps doubling from 10µs to
// 1ms; after ~100ms of waiting the ceiling relaxes to 5ms so long-idle
// connections poll less.

package engine

import (
	"runtime"
	"sync/atomic"
	"time"
)

const (
	// futexPollYields is how many scheduler yields to burn before sleeping.
	futexPollYields = 32
	// futexPollFloor is the first sleep slice.
	futexPollFloor = 5 * time.Microsecond
	// The backoff is three-phase, because cumulative doubling directly
	// quantizes observed wake latency: with a single 1ms ceiling, a waiter
	// whose condition lands ~300µs out wakes on the 10+20+40+80+160+320µs
	// tier boundaries — which showed up verbatim as ~370µs / ~730µs p99
	// plateaus in RPC latency sweeps. Keeping the ceiling at 100µs while a
	// wait is young compresses those plateaus at negligible CPU cost (a
	// wait can spend at most futexPollActivePhase in the shallow phase).
	//
	// futexPollActiveCeiling bounds wake latency while the wait is young
	// (an active connection blocked on the peer's next action).
	futexPollActiveCeiling = 100 * time.Microsecond
	// futexPollActivePhase is how long a wait stays in the shallow phase.
	futexPollActivePhase = 5 * time.Millisecond
	// futexPollCeiling bounds wake latency after the shallow phase.
	futexPollCeiling = 1 * time.Millisecond
	// futexPollIdleAfter is how long a wait runs before it is treated as
	// idle and allowed the relaxed ceiling.
	futexPollIdleAfter = 100 * time.Millisecond
	// futexPollIdleCeiling bounds the poll rate of long-idle waiters.
	futexPollIdleCeiling = 5 * time.Millisecond
)

// futexWait blocks until *addr != val. It may also return early (nil)
// without the value having changed; callers re-validate, exactly as they
// must on Linux where EAGAIN/EINTR return nil.
func futexWait(addr *uint32, val uint32) error {
	return futexPoll(addr, val, time.Time{})
}

// futexWaitTimeout blocks until *addr != val or timeoutNs elapses, in which
// case it returns ErrFutexTimeout. timeoutNs <= 0 means wait indefinitely.
func futexWaitTimeout(addr *uint32, val uint32, timeoutNs int64) error {
	if timeoutNs <= 0 {
		return futexWait(addr, val)
	}
	return futexPoll(addr, val, time.Now().Add(time.Duration(timeoutNs)))
}

// futexWake is a no-op on darwin: pollers notice the store on their next
// re-check, within the backoff ceiling. The return mirrors the Linux
// signature; callers ignore the count.
func futexWake(_ *uint32, _ int) (int, error) {
	return 0, nil
}

// futexPoll is the shared wait loop. A zero deadline means no timeout.
func futexPoll(addr *uint32, val uint32, deadline time.Time) error {
	if atomic.LoadUint32(addr) != val {
		return nil
	}
	for i := 0; i < futexPollYields; i++ {
		runtime.Gosched()
		if atomic.LoadUint32(addr) != val {
			return nil
		}
	}
	start := time.Now()
	sleep := futexPollFloor
	for {
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				if atomic.LoadUint32(addr) != val {
					return nil
				}
				return ErrFutexTimeout
			}
			if sleep > remaining {
				sleep = remaining
			}
		}
		time.Sleep(sleep)
		if atomic.LoadUint32(addr) != val {
			return nil
		}
		waited := time.Since(start)
		ceiling := futexPollActiveCeiling
		if waited >= futexPollIdleAfter {
			ceiling = futexPollIdleCeiling
		} else if waited >= futexPollActivePhase {
			ceiling = futexPollCeiling
		}
		if sleep < ceiling {
			sleep *= 2
			if sleep > ceiling {
				sleep = ceiling
			}
		} else if sleep > ceiling {
			sleep = ceiling
		}
	}
}
