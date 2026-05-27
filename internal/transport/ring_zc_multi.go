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
	"sync/atomic"
)

// Multi-anchor zero-copy infrastructure for the SHM receive path.
//
// Background. The single-anchor implementation (BeginSingleFrameZcCommit /
// EndZcReservation, gated by IsSpeculativeZCEligible's zcActive check) allows
// at most ONE ring-backed mem.Buffer in flight per ring direction. At
// N=100+ concurrent streams sharing one ring this is the dominant bottleneck:
// only the FIRST stream's DATA frame per round becomes ZC; every other
// stream's frame falls to the single-frame copy path, which calls
// shmLpmPool.Get(payloadLen) per message — driving the 38 % mallocgc /
// GC mark CPU profile observed at concurrent 4 KiB jumbo.
//
// Design. Replace the single zcActive/zcDeferredTarget pair with an ordered
// FIFO of anchors (start, end, released). New anchors are appended at the
// tail; consumer Buffer.Free() marks the matching anchor as released.
// On any release, the ring advances header.ReadIdx through the ORDERED
// PREFIX of released anchors (oldest released first; out-of-order release
// is fine but stalls the prefix until the older anchor releases).
//
// Budget. To prevent a slow consumer from pinning unbounded ring memory:
//   - zcMaxAnchors: cap on simultaneous anchors per ring.
//   - zcMaxBytes  : cap on total bytes held by anchors per ring (default
//                   ring.capacity/2 — leaves at least half the ring free
//                   for the writer at all times).
// When either budget is exceeded, BeginAnchor returns nil and the caller
// falls back to the existing single-frame copy path. The copy path is
// strictly correct and inexpensive (one mem.Copy), so the fallback
// degrades gracefully to the pre-multi-anchor performance level.
//
// Concurrency. The anchor queue is protected by zcAnchorsMu. Reader
// thread's Commit (per-frame, hot path) remains lock-free: it loads
// zcActive atomically and bumps zcDeferredTarget via AddUint64 just as
// before. Mutex is only taken on Begin and Release paths, which are
// per-MESSAGE, not per-frame, and already pay a heap allocation for the
// release pool — the mutex cost is negligible relative to that.
//
// Backwards compatibility. zcActive, zcDeferredTarget, zcInFlight, and
// chainOpen are PRESERVED with the same semantics; the multi-anchor path
// just keeps them updated through the new BeginAnchor / ReleaseAnchor
// flows. Existing single-anchor entry points (BeginSingleFrameZcCommit,
// EndZcReservation, AddChainZcInFlight, ReleaseChainZcBuffer) remain in
// place and continue to work — they are simply not used by the production
// h2 codec after this change.

// zcAnchorBudgetCount caps the number of simultaneous ZC anchors held on
// one ring direction. 256 is generous for the expected workload (up to
// ~1000 concurrent streams ping-ponging through one connection) without
// risking unbounded memory if a single anchor is somehow leaked.
const zcAnchorBudgetCount = 256

// zcAnchorMulti tracks a single in-flight ring-backed buffer issued by
// the multi-anchor ZC path. Lifetime: appended to the ring's anchor
// queue on BeginAnchor; marked released by the consumer's Buffer.Free
// callback; finally removed from the queue when the ordered prefix
// advances over it.
type zcAnchorMulti struct {
	start    uint64 // absolute ring index where this anchor's frame starts
	end      uint64 // absolute ring index just past this anchor's frame
	released uint32 // atomic; 1 once the consumer has Freed the matching mem.Buffer
}

// zcMaxBytesBudget returns the max bytes that may be held in-flight by
// the multi-anchor queue. Half of the ring capacity leaves at least 50 %
// of the ring available for the writer at all times, bounding the worst-
// case slow-consumer stall.
func (r *ShmRing) zcMaxBytesBudget() uint64 {
	return r.capacity / 2
}

// BeginAnchor reserves a multi-anchor ZC slot for the contiguous payload
// at [baseIdx, baseIdx+totalBytes) of the ring. Returns the anchor
// pointer on success (caller wraps it in a mem.Buffer and passes the
// anchor to the release pool); returns nil if the budget is exceeded
// (caller falls back to the single-frame copy path).
//
// On success this method also:
//   - bumps zcDeferredTarget by totalBytes (consistent with the
//     single-anchor BeginSingleFrameZcCommit semantics so that
//     subsequent reader-thread Commits AddUint64 onto a coherent value),
//   - sets zcActive = 1 if this is the first anchor in the queue,
//   - increments zcInFlight for backwards-compat with the
//     ReleaseChainZcBuffer-style refcount used by zcChainReleasePool.
func (r *ShmRing) BeginAnchor(baseIdx uint64, totalBytes int) *zcAnchorMulti {
	if totalBytes <= 0 {
		return nil
	}

	r.zcAnchorsMu.Lock()

	if len(r.zcAnchors) >= zcAnchorBudgetCount ||
		r.zcAnchorsBytes+uint64(totalBytes) > r.zcMaxBytesBudget() {
		r.zcAnchorsMu.Unlock()
		atomic.AddUint64(&shmZCAnchorBudgetExceeded, 1)
		return nil
	}

	a := &zcAnchorMulti{
		start: baseIdx,
		end:   baseIdx + uint64(totalBytes),
	}
	r.zcAnchors = append(r.zcAnchors, a)
	r.zcAnchorsBytes += uint64(totalBytes)

	if len(r.zcAnchors) == 1 {
		// First anchor: initialise zcDeferredTarget to this anchor's end.
		// Matches BeginSingleFrameZcCommit's existing semantics.
		atomic.StoreUint64(&r.zcDeferredTarget, a.end)
		atomic.StoreUint32(&r.zcActive, 1)
	} else {
		// Additional anchor: extend zcDeferredTarget by this frame's bytes.
		// The reader thread is single-threaded so any intervening Commit
		// (non-ZC frame) bumps zcDeferredTarget independently; this Add
		// captures the bytes the consumer will hold under THIS anchor.
		atomic.AddUint64(&r.zcDeferredTarget, uint64(totalBytes))
	}

	atomic.AddInt64(&r.zcInFlight, 1)
	r.zcAnchorsMu.Unlock()
	return a
}

// ReleaseAnchor marks an anchor as consumer-released and walks the
// ordered prefix of the anchor queue, advancing header.ReadIdx through
// any newly-released anchors at the head. Safe to call after the ring
// has been closed (silently no-ops).
//
// Out-of-order release is supported: marking anchor B released while
// anchor A (older) is still held just stalls the prefix advance until
// A also releases. No deadlock — A's release will eventually walk over
// both.
func (r *ShmRing) ReleaseAnchor(a *zcAnchorMulti) {
	if a == nil {
		return
	}
	atomic.StoreUint32(&a.released, 1)
	atomic.AddInt64(&r.zcInFlight, -1)

	if isRingClosed(r) {
		return
	}

	r.zcAnchorsMu.Lock()

	// Advance through the ordered prefix of released anchors.
	advanced := false
	for len(r.zcAnchors) > 0 && atomic.LoadUint32(&r.zcAnchors[0].released) == 1 {
		head := r.zcAnchors[0]
		r.zcAnchorsBytes -= (head.end - head.start)
		r.zcAnchors = r.zcAnchors[1:]
		advanced = true
	}

	if !advanced {
		r.zcAnchorsMu.Unlock()
		return
	}

	// Determine how far to advance header.ReadIdx.
	var publishTo uint64
	clearActive := false
	if len(r.zcAnchors) == 0 {
		// All anchors released — publish full deferred target, clear active.
		publishTo = atomic.LoadUint64(&r.zcDeferredTarget)
		clearActive = true
	} else {
		// Still-held anchor at the head — publish up to its start.
		// Bytes < that start are either released anchors' data or
		// non-ZC frames committed during ZC hold (both safe to free).
		publishTo = r.zcAnchors[0].start
	}
	r.zcAnchorsMu.Unlock()

	hdr := r.header()
	if publishTo > 0 {
		r.publishTarget(hdr, publishTo)
	}

	if clearActive {
		atomic.StoreUint32(&r.zcActive, 0)
		// Race fixup: a reader-thread Commit may have observed
		// zcActive=1 and bumped zcDeferredTarget AFTER our Load but
		// BEFORE our Store. Re-read and re-publish if it grew.
		// Once zcActive is 0 future Commits go to the CAS path, so a
		// single refresh suffices.
		refreshed := atomic.LoadUint64(&r.zcDeferredTarget)
		if refreshed > publishTo {
			r.publishTarget(hdr, refreshed)
		}
	}

	// Wake the writer if it was waiting on space. Mirrors the wake
	// performed by Commit's CAS-publish path and by EndZcReservation;
	// without this the writer can miss the freed bytes until the next
	// independent commit on this ring.
	if hdr.ContigWaiters() > 0 {
		hdr.IncrementContigSequence()
		r.signalContig(&hdr.contigSeq)
	}
	if hdr.SpaceWaiters() > 0 {
		hdr.IncrementSpaceSequence()
		r.signalSpace(&hdr.spaceSeq)
	}
}
