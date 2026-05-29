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

// Temporary diagnostic counters used to investigate why TightBufferPool's
// Put rate is far below its Get rate on the SHM Jumbo32 1000-stream × 4 KiB
// concurrent ping-pong bench cell. Will be removed once the cause is
// identified and fixed.

package mem

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var bufferDiagEnabled atomic.Bool

// EnableBufferDiag turns on counter tracking inside NewBuffer/Free.
// MUST be called BEFORE any NewBuffer calls happen in the workload
// (the counters short-circuit on the disabled fast path).
func EnableBufferDiag() {
	bufferDiagEnabled.Store(true)
}

// ResetBufferDiag clears accumulated counters. Useful between bench
// warmup and the measured run.
func ResetBufferDiag() {
	bufferDiag.mu.Lock()
	defer bufferDiag.mu.Unlock()
	bufferDiag.wrapsByCaller = make(map[uintptr]*atomic.Uint64)
	bufferDiag.freeZeroByCaller = make(map[uintptr]*atomic.Uint64)
	bufferDiag.wrapTotal.Store(0)
	bufferDiag.freeZeroTotal.Store(0)
	bufferDiag.refBumps.Store(0)
	bufferDiag.freeNonZero.Store(0)
}

// BufferDiagDump returns a human-readable summary keyed by NewBuffer
// caller site.
func BufferDiagDump() string {
	bufferDiag.mu.Lock()
	defer bufferDiag.mu.Unlock()
	type row struct {
		pc        uintptr
		wraps     uint64
		freeZeros uint64
	}
	rows := make([]row, 0, len(bufferDiag.wrapsByCaller))
	for pc, ctr := range bufferDiag.wrapsByCaller {
		w := ctr.Load()
		fz := uint64(0)
		if f := bufferDiag.freeZeroByCaller[pc]; f != nil {
			fz = f.Load()
		}
		rows = append(rows, row{pc, w, fz})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].wraps > rows[j].wraps })

	var b strings.Builder
	wrapTotal := bufferDiag.wrapTotal.Load()
	freeZeroTotal := bufferDiag.freeZeroTotal.Load()
	refBumps := bufferDiag.refBumps.Load()
	freeNonZero := bufferDiag.freeNonZero.Load()
	freeTotal := freeZeroTotal + freeNonZero
	fmt.Fprintf(&b, "=== mem.Buffer diag ===\n")
	fmt.Fprintf(&b, "NewBuffer wraps:     %12d\n", wrapTotal)
	fmt.Fprintf(&b, "Free → 0 (Put):      %12d  (%.1f%% of wraps; leak = wraps - this)\n",
		freeZeroTotal, 100.0*float64(freeZeroTotal)/float64(max(wrapTotal, 1)))
	fmt.Fprintf(&b, "Free → >0:           %12d\n", freeNonZero)
	fmt.Fprintf(&b, "Total Frees:         %12d  (expected: wraps + refBumps)\n", freeTotal)
	fmt.Fprintf(&b, "Ref bumps:           %12d  (expected total Frees - wraps)\n", refBumps)
	fmt.Fprintf(&b, "Expected Frees:      %12d\n", wrapTotal+refBumps)
	fmt.Fprintf(&b, "Missing Frees:       %12d  (expected - actual; > 0 ⇒ leaked refs)\n",
		int64(wrapTotal+refBumps)-int64(freeTotal))
	fmt.Fprintf(&b, "\nPer caller (top 20 by wraps):\n")
	fmt.Fprintf(&b, "%-12s %12s %12s %10s  %s\n", "pc", "wraps", "freeZeros", "leak%", "site")
	for i, r := range rows {
		if i >= 20 {
			fmt.Fprintf(&b, "  ... (%d more sites)\n", len(rows)-20)
			break
		}
		fn := runtime.FuncForPC(r.pc)
		site := "?"
		if fn != nil {
			file, line := fn.FileLine(r.pc)
			site = fmt.Sprintf("%s\n             %s:%d", fn.Name(), file, line)
		}
		leakPct := 100.0 - 100.0*float64(r.freeZeros)/float64(max(r.wraps, 1))
		fmt.Fprintf(&b, "%-12x %12d %12d %9.1f%%  %s\n",
			r.pc, r.wraps, r.freeZeros, leakPct, site)
	}
	return b.String()
}

var bufferDiag = struct {
	mu               sync.Mutex
	wrapsByCaller    map[uintptr]*atomic.Uint64
	freeZeroByCaller map[uintptr]*atomic.Uint64
	wrapTotal        atomic.Uint64
	freeZeroTotal    atomic.Uint64
	refBumps         atomic.Uint64
	freeNonZero      atomic.Uint64
}{
	wrapsByCaller:    make(map[uintptr]*atomic.Uint64),
	freeZeroByCaller: make(map[uintptr]*atomic.Uint64),
}

func bufferDiagCounter(m map[uintptr]*atomic.Uint64, pc uintptr) *atomic.Uint64 {
	bufferDiag.mu.Lock()
	c, ok := m[pc]
	if !ok {
		c = new(atomic.Uint64)
		m[pc] = c
	}
	bufferDiag.mu.Unlock()
	return c
}

// diagOnWrap is called from NewBuffer when wrapping a buffer with a non-nil
// pool. Records the caller's PC for per-site attribution.
func diagOnWrap(callerPC uintptr) {
	if !bufferDiagEnabled.Load() {
		return
	}
	bufferDiag.wrapTotal.Add(1)
	bufferDiagCounter(bufferDiag.wrapsByCaller, callerPC).Add(1)
}

// diagOnFreeZero is called from (*buffer).Free when the refcount drops to 0,
// just before pool.Put. Records under the original NewBuffer caller's PC if
// known.
func diagOnFreeZero(originPC uintptr) {
	if !bufferDiagEnabled.Load() {
		return
	}
	bufferDiag.freeZeroTotal.Add(1)
	bufferDiagCounter(bufferDiag.freeZeroByCaller, originPC).Add(1)
}

// diagOnFreeNonZero is called from (*buffer).Free when the refcount drops
// but stays above 0. Used to estimate "expected total Frees" vs observed.
func diagOnFreeNonZero() {
	if !bufferDiagEnabled.Load() {
		return
	}
	bufferDiag.freeNonZero.Add(1)
}

// diagOnRef is called from (*buffer).Ref. Used to estimate "expected total
// Frees" vs observed.
func diagOnRef() {
	if !bufferDiagEnabled.Load() {
		return
	}
	bufferDiag.refBumps.Add(1)
}

// diagCallerPC returns the program counter of the caller of NewBuffer.
// Skip = 2 (this fn + NewBuffer itself).
func diagCallerPC() uintptr {
	if !bufferDiagEnabled.Load() {
		return 0
	}
	var pcs [1]uintptr
	n := runtime.Callers(3, pcs[:])
	if n == 0 {
		return 0
	}
	return pcs[0]
}
