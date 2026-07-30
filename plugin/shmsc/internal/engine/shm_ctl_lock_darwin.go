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

// Control-segment cross-process serialization lock (darwin).
//
// Byte-for-byte the Linux implementation: darwin has BSD flock(2) with the
// same LOCK_EX/LOCK_NB/LOCK_UN semantics, and every x/sys/unix symbol used
// here exists on darwin. The file is a copy rather than a widened tag only
// because the `_linux.go` filename suffix pins the original to GOOS=linux.
//
// See shm_ctl_lock_linux.go for the full rationale: the control segment's
// Ring A is shared by every client connecting to the same server, so the
// SPSC assumption only holds while one client's CONNECT/ACCEPT exchange is
// serialized under this lock. Leaving darwin on the process-local stub
// would silently break that guarantee across processes.

package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// acquireControlLock acquires an exclusive flock on a sibling ".lock"
// file next to the named control segment. It blocks until the lock is
// granted or ctx is cancelled. The returned release function MUST be
// invoked once the CONNECT/ACCEPT exchange completes; the closure is
// idempotent.
func acquireControlLock(ctx context.Context, ctlName string) (func(), error) {
	lockPath := generateSegmentPath(ctlName) + ".lock"

	// Open (creating if absent) with restrictive 0600 mode. The file
	// has no payload; flock state lives in the kernel and is keyed by
	// the underlying inode.
	fd, err := unix.Open(lockPath, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("shm: open control lock %q: %w", lockPath, err)
	}

	// Tighten permissions defensively in case a prior process created
	// the file with a looser umask.
	_ = unix.Fchmod(fd, 0o600)

	// Acquire LOCK_EX via non-blocking flock in a polling loop so ctx
	// cancellation is honoured promptly. Contention here only occurs
	// during connection establishment, off the data-plane hot path.
	const pollInterval = 5 * time.Millisecond
	pollTimer := time.NewTimer(pollInterval)
	defer pollTimer.Stop()
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			unix.Close(fd)
			return nil, fmt.Errorf("shm: flock control lock %q: %w", lockPath, err)
		}
		// Lock held by another client; wait and retry.
		pollTimer.Reset(pollInterval)
		select {
		case <-ctx.Done():
			unix.Close(fd)
			return nil, fmt.Errorf("shm: control lock %q: %w", lockPath, ctx.Err())
		case <-pollTimer.C:
		}
	}

	var released atomic.Bool
	return func() {
		if released.Swap(true) {
			return
		}
		// LOCK_UN is implicit on close, but explicit unlock first makes
		// the wake of any waiting peer prompt.
		_ = unix.Flock(fd, unix.LOCK_UN)
		_ = unix.Close(fd)
	}, nil
}

// removeControlLock best-effort unlinks the lock file. It is called by
// the listener at shutdown after the control segment itself has been
// removed; failure is silently ignored.
func removeControlLock(ctlName string) {
	_ = os.Remove(generateSegmentPath(ctlName) + ".lock")
}
