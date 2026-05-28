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

import "sync"

// zcMultiAnchorReleasePool implements mem.BufferPool for the multi-
// anchor single-frame ZC fast path. Each ZC buffer wraps the ring
// slice in a mem.Buffer backed by a fresh pool instance that captures
// the corresponding MultiAnchor. Buffer.Free → pool.Put → anchor.Release
// triggers the prefix-walk that advances header.ReadIdx.
//
// Why a per-ZC pool instance: mem.BufferPool's Put receives only the
// *[]byte being returned; there is no per-Buffer state on the
// mem.Buffer struct to carry the anchor pointer. The cheapest way to
// associate "this Buffer's release means anchor X" is to give each
// Buffer its own pool struct whose state IS the anchor.
//
// Allocation cost: 24 bytes per pool struct, recycled via a process-
// global sync.Pool, so steady-state amortizes to ≈ 0 allocations per
// ZC under reuse. Get returns a heap allocation matching the legacy
// zcChainReleasePool's behaviour (occasionally the caller asks the
// pool for a grow-buffer; that allocation has nothing to do with the
// ring slice and is independent).
//
// CRITICAL: Put MUST be idempotent — gRPC's mem.Buffer.Free is
// supposed to be called exactly once but we guard against
// double-free by nilling anchor before recycling. A second Put
// observes anchor==nil and no-ops.

type zcMultiAnchorReleasePool struct {
	ring   *ShmRing
	anchor *MultiAnchor
}

var zcMultiAnchorReleasePoolSync = sync.Pool{
	New: func() any { return &zcMultiAnchorReleasePool{} },
}

func newZcMultiAnchorReleasePool(ring *ShmRing, anchor *MultiAnchor) *zcMultiAnchorReleasePool {
	p := zcMultiAnchorReleasePoolSync.Get().(*zcMultiAnchorReleasePool)
	p.ring = ring
	p.anchor = anchor
	return p
}

func (p *zcMultiAnchorReleasePool) Get(n int) *[]byte {
	buf := make([]byte, n)
	return &buf
}

func (p *zcMultiAnchorReleasePool) Put(_ *[]byte) {
	if p == nil || p.anchor == nil {
		return
	}
	anchor := p.anchor
	ring := p.ring
	p.anchor = nil
	p.ring = nil
	// Recycle the pool struct AFTER nilling our copies of the fields so
	// a racing double-Put can't observe a partially-recycled pool.
	defer zcMultiAnchorReleasePoolSync.Put(p)
	if ring == nil || isRingClosed(ring) {
		// Ring closed: skip the actual release call (segment may be
		// unmapped). Slot state remains "released" but the anchor never
		// gets prefix-walked; that is acceptable at shutdown.
		return
	}
	anchor.Release()
}
