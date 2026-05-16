//go:build linux

/*
 *
 * Copyright 2025 gRPC authors.
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
	"os"
	"strings"
	"testing"
)

// TestShmFDCount documents the per-connection file-descriptor footprint
// of the SHM transport on Linux, in response to reviewer (Doug) feedback
// asking us to quantify FD usage vs TCP / UDS. It prints the FD delta
// around opening / closing a single SHM segment so reviewers can read
// the number from `go test -v -run TestShmFDCount`.
//
// Persistent FDs per SHM connection per process, on Linux:
//
//   1 × FD  → /dev/shm/grpc_shm_<name>  — the mmap'd segment file.
//                                         Kept open for resize and
//                                         unlink-on-close semantics.
//   0 × eventfd — SHM uses native futex(2) on a uint32 in shared
//                 memory, not eventfd, so no per-direction FD is
//                 needed for wake-ups.
//   0 × extras after mmap; both rings reuse the same mapping.
//
// Total: ONE FD per process per connection (data plane). If a security
// handshake is established on a separate control segment, that adds ONE
// more FD, for a total of 2.
//
// Compare to:
//   TCP loopback : 1 socket FD per side
//   UDS          : 1 socket FD per side
//
// So SHM is parity with TCP/UDS for the data plane. Reviewers sizing
// ulimit -n for N concurrent SHM connections should budget the same FD
// count as for N TCP connections, plus N if security handshake is used.
//
// The reviewer's underlying concern is FD exhaustion under many
// concurrent shmem connections (e.g. a service-mesh sidecar with
// thousands of peers). The answer is: same order of magnitude as TCP,
// modest predictable overhead.
func TestShmFDCount(t *testing.T) {
	before := snapshotProcFDs(t)
	if before == nil {
		return // skipped on non-Linux
	}

	segName := testSegName("fd_count")
	defer RemoveSegment(segName)

	seg, err := CreateSegment(segName, 1*1024*1024, 1*1024*1024)
	if err != nil {
		t.Fatalf("CreateSegment: %v", err)
	}

	during := snapshotProcFDs(t)
	delta := fdDelta(before, during)
	shmFDs := 0
	for fd, target := range delta {
		t.Logf("  fd %s -> %s", fd, target)
		if strings.Contains(target, "grpc_shm_") || strings.Contains(target, "/dev/shm/") {
			shmFDs++
		}
	}
	t.Logf("Total FDs opened by CreateSegment: %d", len(delta))
	t.Logf("Of those backed by /dev/shm/grpc_shm_*: %d", shmFDs)

	if shmFDs != 1 {
		t.Errorf("expected exactly 1 /dev/shm FD per SHM segment; got %d", shmFDs)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("seg.Close: %v", err)
	}

	after := snapshotProcFDs(t)
	leaked := fdDelta(before, after)
	if len(leaked) > 0 {
		for fd, target := range leaked {
			t.Errorf("FD leaked after segment close: fd %s -> %s", fd, target)
		}
	}
}

// snapshotProcFDs returns a map of fd → readlink target for every entry
// in /proc/self/fd. Returns nil and skips the test on non-Linux.
func snapshotProcFDs(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("FD probe needs /proc/self/fd (Linux only): %v", err)
		return nil
	}
	out := make(map[string]string, len(entries))
	for _, e := range entries {
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil {
			continue
		}
		out[e.Name()] = target
	}
	return out
}

// fdDelta returns the entries present in after but not in before.
func fdDelta(before, after map[string]string) map[string]string {
	delta := make(map[string]string)
	for fd, target := range after {
		if _, had := before[fd]; !had {
			delta[fd] = target
		}
	}
	return delta
}
