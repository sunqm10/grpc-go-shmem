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

// Temporary diagnostic helpers for tightBufferPool. NOT for production use —
// these helpers exist to investigate the "pool miss rate" puzzle observed in
// the Jumbo32 1000-stream × 4 KiB bench cell where alloc_space showed
// tightBufferPool.Get fall-through to make() at ~50 % of Get calls despite
// the size-aware cap covering the expected in-flight working set.
//
// This file will be removed once the diagnostic data identifies the cause.

package experimental

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// tightBufferPoolDiagCounters tracks per-size Get / hit / miss counts across
// the active *tightBufferPool instance. Counters are global (sum across all
// instances) because the bench wires up two TightBufferPool instances (one
// server, one client) and we want their aggregate behaviour. Per-size
// breakdown lets us spot bimodal size distributions.
var tightBufferPoolDiagCounters = struct {
	mu      sync.Mutex
	gets    map[int]*atomic.Uint64
	hits    map[int]*atomic.Uint64
	misses  map[int]*atomic.Uint64
	puts    map[int]*atomic.Uint64 // Put attempts (regardless of accept/drop)
	accepts map[int]*atomic.Uint64 // Put accepted (pool had room)
	drops   map[int]*atomic.Uint64 // Put dropped (pool was full)
	enabled atomic.Bool
}{
	gets:    make(map[int]*atomic.Uint64),
	hits:    make(map[int]*atomic.Uint64),
	misses:  make(map[int]*atomic.Uint64),
	puts:    make(map[int]*atomic.Uint64),
	accepts: make(map[int]*atomic.Uint64),
	drops:   make(map[int]*atomic.Uint64),
}

// EnableTightBufferPoolDiag turns on per-size hit/miss counter tracking
// inside tightBufferPool.Get. Must be called BEFORE any Get calls (the
// counters are sampled at every Get).
func EnableTightBufferPoolDiag() {
	tightBufferPoolDiagCounters.enabled.Store(true)
}

// ResetTightBufferPoolDiag zeroes all counters. Useful between bench
// iterations to isolate steady-state behaviour from warmup.
func ResetTightBufferPoolDiag() {
	tightBufferPoolDiagCounters.mu.Lock()
	defer tightBufferPoolDiagCounters.mu.Unlock()
	tightBufferPoolDiagCounters.gets = make(map[int]*atomic.Uint64)
	tightBufferPoolDiagCounters.hits = make(map[int]*atomic.Uint64)
	tightBufferPoolDiagCounters.misses = make(map[int]*atomic.Uint64)
	tightBufferPoolDiagCounters.puts = make(map[int]*atomic.Uint64)
	tightBufferPoolDiagCounters.accepts = make(map[int]*atomic.Uint64)
	tightBufferPoolDiagCounters.drops = make(map[int]*atomic.Uint64)
}

// TightBufferPoolDiagDump returns a human-readable summary of the counters,
// sorted by Get count descending. Empty string if diagnostics weren't
// enabled (no entries were recorded).
func TightBufferPoolDiagDump() string {
	tightBufferPoolDiagCounters.mu.Lock()
	defer tightBufferPoolDiagCounters.mu.Unlock()
	if len(tightBufferPoolDiagCounters.gets) == 0 {
		return ""
	}
	type row struct {
		size                                       int
		gets, hits, misses, puts, accepts, drops uint64
	}
	rows := make([]row, 0, len(tightBufferPoolDiagCounters.gets))
	var totalGets, totalHits, totalMisses, totalPuts, totalAccepts, totalDrops uint64
	for size, getCtr := range tightBufferPoolDiagCounters.gets {
		gets := getCtr.Load()
		hits := uint64(0)
		if h := tightBufferPoolDiagCounters.hits[size]; h != nil {
			hits = h.Load()
		}
		misses := uint64(0)
		if m := tightBufferPoolDiagCounters.misses[size]; m != nil {
			misses = m.Load()
		}
		puts := uint64(0)
		if p := tightBufferPoolDiagCounters.puts[size]; p != nil {
			puts = p.Load()
		}
		accepts := uint64(0)
		if a := tightBufferPoolDiagCounters.accepts[size]; a != nil {
			accepts = a.Load()
		}
		drops := uint64(0)
		if d := tightBufferPoolDiagCounters.drops[size]; d != nil {
			drops = d.Load()
		}
		rows = append(rows, row{size, gets, hits, misses, puts, accepts, drops})
		totalGets += gets
		totalHits += hits
		totalMisses += misses
		totalPuts += puts
		totalAccepts += accepts
		totalDrops += drops
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].gets > rows[j].gets })

	var b strings.Builder
	hitPct := 100.0 * float64(totalHits) / float64(max(totalGets, 1))
	putGetRatio := 100.0 * float64(totalPuts) / float64(max(totalGets, 1))
	dropPct := 100.0 * float64(totalDrops) / float64(max(totalPuts, 1))
	fmt.Fprintf(&b, "=== tightBufferPool diag: %d size classes ===\n", len(rows))
	fmt.Fprintf(&b, "Gets:    %12d  (%d hits / %d misses = %.1f%% hit rate)\n",
		totalGets, totalHits, totalMisses, hitPct)
	fmt.Fprintf(&b, "Puts:    %12d  (%.1f%% of Gets — should be ~100%% if Free path works!)\n",
		totalPuts, putGetRatio)
	fmt.Fprintf(&b, "         %12d accepts, %d drops (%.1f%% of Puts dropped due to pool-full)\n",
		totalAccepts, totalDrops, dropPct)
	fmt.Fprintf(&b, "%-10s %12s %12s %12s %10s %12s %12s\n", "size", "gets", "hits", "misses", "hit%", "puts", "drops")
	for i, r := range rows {
		if i >= 25 { // top 25 only
			fmt.Fprintf(&b, "  ... (%d more size classes)\n", len(rows)-25)
			break
		}
		rowHit := 100.0 * float64(r.hits) / float64(max(r.gets, 1))
		fmt.Fprintf(&b, "%-10d %12d %12d %12d %9.1f%% %12d %12d\n",
			r.size, r.gets, r.hits, r.misses, rowHit, r.puts, r.drops)
	}
	return b.String()
}

// diagCounter returns the per-size *atomic.Uint64 for the given map and size,
// creating it under the lock if absent. Hot path is the Load fast case which
// avoids the lock entirely.
func diagCounter(m map[int]*atomic.Uint64, size int) *atomic.Uint64 {
	tightBufferPoolDiagCounters.mu.Lock()
	c, ok := m[size]
	if !ok {
		c = new(atomic.Uint64)
		m[size] = c
	}
	tightBufferPoolDiagCounters.mu.Unlock()
	return c
}

// diagRecord is called from tightBufferPool.Get on every call. Fast path
// (when diagnostics are disabled) is a single atomic.Bool load.
func diagRecord(size int, hit bool) {
	if !tightBufferPoolDiagCounters.enabled.Load() {
		return
	}
	diagCounter(tightBufferPoolDiagCounters.gets, size).Add(1)
	if hit {
		diagCounter(tightBufferPoolDiagCounters.hits, size).Add(1)
	} else {
		diagCounter(tightBufferPoolDiagCounters.misses, size).Add(1)
	}
}

// diagRecordPut is called from tightBufferPool.Put. accepted=true means
// the buffer was successfully added to the free list; false means dropped
// because the list was already at cap.
func diagRecordPut(size int, accepted bool) {
	if !tightBufferPoolDiagCounters.enabled.Load() {
		return
	}
	diagCounter(tightBufferPoolDiagCounters.puts, size).Add(1)
	if accepted {
		diagCounter(tightBufferPoolDiagCounters.accepts, size).Add(1)
	} else {
		diagCounter(tightBufferPoolDiagCounters.drops, size).Add(1)
	}
}
