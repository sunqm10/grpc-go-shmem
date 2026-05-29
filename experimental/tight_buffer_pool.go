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

package experimental

import (
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/mem"
)

// TightBufferPool returns a mem.BufferPool that returns buffers of exactly
// the requested size rather than the next-power-of-two tier the default
// BinaryTieredBufferPool returns. This eliminates the per-Get overshoot
// the default pool incurs on payloads just above a tier boundary — the
// canonical example being a 4 KiB protobuf message snapping to the 16 KiB
// tier (4× overshoot) on every Marshal.
//
// The shared-memory transport benchmark profile shows that overshoot is
// responsible for the majority of marshal-side allocation pressure on
// small-payload workloads (about 64 % of total alloc_space). Replacing
// the default pool with a TightBufferPool eliminates that pressure
// without changing wire-format semantics or codec selection.
//
// Trade-off: every distinct requested size becomes its own pool, so
// workloads with high size variability create many pools. To bound memory,
// the pool caps the number of distinct size classes at maxSizeClasses
// (default 1024). Get requests for new size classes beyond the cap fall
// back to a single shared sync.Pool keyed by a coarser power-of-two
// bucket, which preserves the BinaryTieredBufferPool overshoot for those
// uncommon sizes but does not block them.
//
// Concurrency: the pool is safe for concurrent use. Internally it uses
// a sync.Map of per-size bounded strong-ref free lists, so Get / Put on
// the hot path is lock-free read + a single mutex-guarded append/pop.
// First-time allocation of a new size class takes one LoadOrStore on
// the inner sync.Map.
//
// Backing implementation: each size class is a bounded strong-ref free
// list (NOT sync.Pool). sync.Pool migrates entries to a "victim cache"
// on every GC cycle and drops them entirely on the GC after that,
// capping pooled-buffer lifetime at roughly 2 GC periods. At sustained
// high allocation rates — e.g. the Jumbo32 1000-stream × 4 KiB
// concurrent ping-pong bench, where this pool sees ≈ 300 K ops/sec —
// the drain schedule causes hit-rate to collapse and forms a positive
// feedback loop (high alloc rate → frequent GC → pool drained →
// makeslice fires → more alloc → more GC). Profiles showed mallocgc
// at 45 % cum CPU. Switching to a GC-decoupled bounded free list
// (the C# ArrayPool<byte>.Shared equivalent) breaks the loop while
// adding only ≈ 50 ns mutex hold per op.
//
// Resident memory is bounded by maxSizeClasses × defaultTightFreeListCap
// × max-tier-size. With defaults this is ≈ 1024 × 1024 × 1 MiB worst
// case if every size class saturates, but realistic workloads cluster
// around a handful of message sizes and occupy a small fraction.
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a
// later release.
func TightBufferPool() mem.BufferPool {
	return newTightBufferPool(defaultTightPoolMaxSizeClasses)
}

const (
	defaultTightPoolMaxSizeClasses = 1024
	// defaultTightFreeListCap bounds the per-size-class strong-ref free
	// list. Sized to cover one outstanding buffer per concurrent stream
	// at the published high-concurrency bench targets (N=1000) with
	// modest headroom. See strong_ref_buffer_pool.go in internal/mem
	// for the equivalent constant rationale.
	//
	// DIAG bump 2026-05-29: raised from 1024 → 8192 to confirm whether
	// pool capacity is the dominant driver of mallocgc / gcAssistAlloc
	// CPU on the Jumbo32 1000-stream × 4 KiB bench cell. Diagnostic
	// counters showed 14 % hit rate at cap=1024 → in-flight working set
	// far exceeds cap → 86 % Puts dropped → 290 MB/s GC garbage from
	// dropped buffers. If 8192 lifts hit rate to >90 % and throughput
	// rises, GC pressure was indeed a real driver and the cap should be
	// permanently raised (or made adaptive). If throughput is flat, GC
	// pressure is not the bottleneck and the scheduler (doneCh park) is
	// the dominant cost — confirming D-NEW as the required fix.
	//
	// 4 KiB hot tier at cap 8192 ≈ 32 MiB resident memory.
	defaultTightFreeListCap = 8192
)

// boundedFreeList is a per-size-class bounded strong-ref free list.
// It replaces sync.Pool to decouple pool-hit-rate from GC frequency.
// Entries are retained indefinitely up to capN; surplus Put-s are
// dropped and left for GC. See the doc comment on TightBufferPool
// for the rationale.
type boundedFreeList struct {
	mu   sync.Mutex
	free []*[]byte
	capN int
}

func newBoundedFreeList(capN int) *boundedFreeList {
	return &boundedFreeList{capN: capN}
}

// pop returns the most recently pushed buffer, or nil if empty.
func (l *boundedFreeList) pop() *[]byte {
	l.mu.Lock()
	if n := len(l.free); n > 0 {
		b := l.free[n-1]
		l.free[n-1] = nil
		l.free = l.free[:n-1]
		l.mu.Unlock()
		return b
	}
	l.mu.Unlock()
	return nil
}

// push retains the buffer if free-list size is below cap, otherwise
// drops it. Returns true if retained.
func (l *boundedFreeList) push(b *[]byte) bool {
	l.mu.Lock()
	if len(l.free) >= l.capN {
		l.mu.Unlock()
		return false
	}
	l.free = append(l.free, b)
	l.mu.Unlock()
	return true
}

type tightBufferPool struct {
	pools           sync.Map // map[int]*boundedFreeList — key is exact size class
	sizeClassCount  atomic.Int32
	maxSizeClasses  int32
	overflowList    *boundedFreeList // shared fallback when sizeClassCount >= maxSizeClasses
	overflowMinSize int              // smallest capacity ever requested from overflowList
}

func newTightBufferPool(maxSizeClasses int32) *tightBufferPool {
	return &tightBufferPool{
		maxSizeClasses: maxSizeClasses,
		overflowList:   newBoundedFreeList(defaultTightFreeListCap),
	}
}

// Get returns a buffer with capacity exactly equal to size. If a previously
// returned buffer of exactly this size is available in the per-size free
// list it is reused (zero allocation); otherwise a fresh make([]byte, size)
// is performed and returned. The buffer's length is set to size.
func (p *tightBufferPool) Get(size int) *[]byte {
	if size <= 0 {
		empty := make([]byte, 0)
		return &empty
	}
	pool := p.poolFor(size)
	if b := pool.pop(); b != nil {
		if cap(*b) >= size {
			diagRecord(size, true)
			*b = (*b)[:size]
			return b
		}
		// Capacity shrank below request (shouldn't happen — push preserves
		// cap, and free-list entries are keyed by cap). Discard and alloc.
	}
	diagRecord(size, false)
	buf := make([]byte, size)
	return &buf
}

// Put returns the buffer to the pool keyed by its capacity. The buffer's
// length is reset to its capacity so the next Get can re-slice safely.
// Callers must not retain references to the slice after Put.
func (p *tightBufferPool) Put(b *[]byte) {
	if b == nil {
		return
	}
	c := cap(*b)
	if c == 0 {
		return
	}
	*b = (*b)[:c] // restore length so cap is preserved on reuse
	pool := p.poolFor(c)
	accepted := pool.push(b) // drop on overflow is fine — GC will reclaim
	diagRecordPut(c, accepted)
}

// poolFor locates (or creates) the *boundedFreeList dedicated to the given
// size. To avoid unbounded growth on workloads with many distinct sizes,
// the number of pools is capped at p.maxSizeClasses. Once the cap is
// reached new sizes share p.overflowList (which loses the exact-size
// guarantee but never blocks). The cap is a soft limit applied with a
// CAS race — minor over-creation under contention is acceptable.
func (p *tightBufferPool) poolFor(size int) *boundedFreeList {
	if v, ok := p.pools.Load(size); ok {
		return v.(*boundedFreeList)
	}
	if p.sizeClassCount.Load() >= p.maxSizeClasses {
		return p.overflowList
	}
	newList := newBoundedFreeList(defaultTightFreeListCap)
	actual, loaded := p.pools.LoadOrStore(size, newList)
	if !loaded {
		p.sizeClassCount.Add(1)
	}
	return actual.(*boundedFreeList)
}

// Ensure tightBufferPool satisfies mem.BufferPool.
var _ mem.BufferPool = (*tightBufferPool)(nil)
