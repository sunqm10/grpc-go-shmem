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

package mem

import "sync"

// defaultStrongRefPoolCap is the FALLBACK per-tier retention cap when
// the caller doesn't specify one. Most callers should not use this
// directly — prefer strongRefCapForSize(size) which scales cap inversely
// with buffer size to bound total resident memory.
const defaultStrongRefPoolCap = 1024

// strongRefCapForTier returns a per-tier retention cap that scales
// inversely with tier size. The intent: each tier's worst-case resident
// memory is bounded at strongRefMemBudgetPerTier (~16 MiB) so the total
// across all tiers stays near 100–120 MiB for the default 7-tier set.
//
// Why size-aware
//
// SHM transport's send path holds each marshaled message buffer for
// ~15–20 µs (chan enqueue + writer drain + ring write + doneCh) before
// returning it to the pool. At the Jumbo32 1000-stream × 4 KiB cell
// (~172 K msg/s) this puts ~3500 buffers simultaneously in flight on
// the 4 KiB tier. A flat cap of 1024 yields ~30 % pool hit rate; the
// other 70 % become fresh makeslice/GC pressure. Bumping the 4 KiB
// tier cap to 4096 covers the in-flight working set and drives hit
// rate near 100 % on that cell.
//
// Bigger tiers don't need a big cap: at 1 MiB payloads the bench
// runs at ~700 ops/sec on 10–10 K streams, giving in-flight buffer
// count in the dozens, not thousands. Per-tier cap = budget / size
// gives each tier the right amount of headroom without inflating
// memory on tiers that don't need it.
//
// Floors and ceilings keep the policy sane:
//   - cap >= 64: even big tiers retain a working set for warm cycles
//   - cap <= 8192: bound 256-byte tier at 2 MiB (vs unbounded growth)
func strongRefCapForTier(size int) int {
	const (
		budget = 16 * 1024 * 1024 // 16 MiB per tier
		minCap = 64
		maxCap = 8192
	)
	if size <= 0 {
		return defaultStrongRefPoolCap
	}
	cap := budget / size
	if cap < minCap {
		return minCap
	}
	if cap > maxCap {
		return maxCap
	}
	return cap
}

// strongRefSizedPool is a drop-in replacement for sizedBufferPool that
// holds STRONG references to its retained buffers. Unlike sync.Pool —
// whose entries are migrated to a "victim cache" on every GC cycle and
// dropped entirely on the GC after that, capping pooled-buffer lifetime
// at roughly 2 GC periods — strongRefSizedPool retains every entry
// until the bounded cap is reached, at which point Put becomes a no-op
// and the surplus buffer is left for GC.
//
// Why this exists
//
// At high steady-state allocation rates the sync.Pool victim-cache
// drain schedule causes pool hit-rate to collapse: GC fires more
// frequently when the heap grows fast, and each GC empties the pool.
// Profiled on the shared-memory transport's Jumbo32 1000-stream ×
// 4 KiB concurrent ping-pong cell, sync.Pool-backed buffer pools
// produce a positive feedback loop:
//
//	high alloc rate → frequent GC → pool drained → makeslice fires →
//	      more alloc → more GC → ...
//
// This shows up in profiles as outsized `mallocgc` and
// `gcAssistAlloc` cumulative CPU time, well in excess of what the raw
// byte allocation rate would predict. Replacing the sync.Pool backing
// with a bounded strong-ref free list (this type) breaks the loop:
// pool entries survive across arbitrary GC cycles, hit-rate stabilises
// at high throughput, and the allocator stops being the bottleneck.
// The pattern mirrors .NET's ArrayPool<byte>.Shared which is
// likewise GC-decoupled.
//
// Trade-off
//
// strongRefSizedPool uses a single sync.Mutex to guard its slice. At
// the bench cell above (≈ 300 K pool ops/sec) the mutex hold averages
// ≈ 50 ns, so the contention overhead is ≈ 1.5 % of one core — small
// compared to the ≈ 15 % CPU recovered from removed GC pressure.
// sync.Pool's per-P sharded fast path is cheaper per call but pays
// the much larger GC-drain tax that this type avoids. If profiling
// shows the mutex becoming a bottleneck at higher rates, the next
// step is to shard the free list per CPU; the public Get / Put
// surface remains unchanged.
//
// Resident memory is bounded by capN × defaultSize. Buffers beyond
// the cap are dropped on Put (caller's reference is released; the
// runtime will collect the slice on the next GC). The cap is a soft
// runaway guard rather than a routine limit; choose it generously
// enough that steady-state working sets stay below it.
type strongRefSizedPool struct {
	mu          sync.Mutex
	free        []*[]byte // strong references; not subject to sync.Pool victim drain
	capN        int       // max retained buffers; 0 means use defaultStrongRefPoolCap
	defaultSize int
	shouldZero  bool
}

// newStrongRefSizedBufferPool constructs a tier pool that holds buffers
// of exactly defaultSize bytes (capacity). If zero is true the buffer is
// cleared before being returned from Get; otherwise the caller is
// responsible for treating the contents as garbage. cap bounds the
// number of buffers retained; if cap <= 0 the package default is used.
func newStrongRefSizedBufferPool(defaultSize int, zero bool, capN int) *strongRefSizedPool {
	if capN <= 0 {
		capN = defaultStrongRefPoolCap
	}
	return &strongRefSizedPool{
		defaultSize: defaultSize,
		shouldZero:  zero,
		capN:        capN,
	}
}

// Get returns a buffer with the requested length. If the free list is
// non-empty the last-retained buffer is reused (zero alloc, mutex
// hold ≈ 50 ns). Otherwise a fresh make([]byte, size, defaultSize) is
// performed. When shouldZero is set the returned buffer's full
// capacity is cleared before return.
func (p *strongRefSizedPool) Get(size int) *[]byte {
	p.mu.Lock()
	if n := len(p.free); n > 0 {
		b := p.free[n-1]
		p.free[n-1] = nil // drop strong ref so the buffer can be GC'd if caller never returns it
		p.free = p.free[:n-1]
		p.mu.Unlock()
		buf := *b
		if p.shouldZero {
			clear(buf[:cap(buf)])
		}
		*b = buf[:size]
		return b
	}
	p.mu.Unlock()
	buf := make([]byte, size, p.defaultSize)
	return &buf
}

// Put returns a buffer to the pool. Buffers smaller than defaultSize
// are dropped (they belong in a smaller tier and would corrupt this
// tier's size invariant). When the free list is at capacity, the
// buffer is dropped and left for GC.
func (p *strongRefSizedPool) Put(b *[]byte) {
	if b == nil {
		return
	}
	if cap(*b) < p.defaultSize {
		// Wrong tier; ignore. Matches sizedBufferPool.Put semantics.
		return
	}
	p.mu.Lock()
	if len(p.free) >= p.capN {
		p.mu.Unlock()
		return
	}
	p.free = append(p.free, b)
	p.mu.Unlock()
}

// NewStrongRefDirtyBinaryTieredBufferPool returns a BufferPool that
// behaves like NewDirtyBinaryTieredBufferPool but whose per-tier
// retention is a bounded strong-ref free list instead of sync.Pool.
//
// Use this when the calling subsystem has sustained allocation rates
// that produce visible sync.Pool victim-cache drain pressure in the
// profile (e.g. mallocgc ≥ 30 % cumulative under a hot-path benchmark).
// At lower throughputs sync.Pool's per-P sharded fast path is cheaper.
//
// Per-tier cap is sized by strongRefCapForTier(size) which scales
// inversely with tier size: small tiers (e.g. 4 KiB) get cap=4096 to
// cover SHM's ~3500-buffer in-flight working set at 1000-stream 4 KiB
// concurrent loads, while large tiers (e.g. 1 MiB) get cap=64. Total
// resident memory is bounded at ~16 MiB per tier (~120 MiB across the
// default 7-tier set).
func NewStrongRefDirtyBinaryTieredBufferPool(powerOfTwoExponents ...uint8) (*BinaryTieredBufferPool, error) {
	return newBinaryTiered(func(size int) bufferPool {
		return newStrongRefSizedBufferPool(size, false, strongRefCapForTier(size))
	}, NewDirtySimplePool(), powerOfTwoExponents...)
}

// NewStrongRefBinaryTieredBufferPool is the zeroing variant of
// NewStrongRefDirtyBinaryTieredBufferPool. Buffers are cleared before
// being returned from Get.
func NewStrongRefBinaryTieredBufferPool(powerOfTwoExponents ...uint8) (*BinaryTieredBufferPool, error) {
	return newBinaryTiered(func(size int) bufferPool {
		return newStrongRefSizedBufferPool(size, true, strongRefCapForTier(size))
	}, &SimpleBufferPool{shouldZero: true}, powerOfTwoExponents...)
}
