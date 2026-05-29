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

import (
	"runtime"
	"sync"
	"testing"
)

func TestStrongRefSizedPool_GetPutReuse(t *testing.T) {
	p := newStrongRefSizedBufferPool(1024, false, 8)
	b1 := p.Get(100)
	if got := cap(*b1); got != 1024 {
		t.Fatalf("Get returned cap %d, want 1024", got)
	}
	if got := len(*b1); got != 100 {
		t.Fatalf("Get returned len %d, want 100", got)
	}
	// Mark the slice so we can detect reuse.
	(*b1)[0] = 0x42
	p.Put(b1)

	b2 := p.Get(50)
	if got := cap(*b2); got != 1024 {
		t.Fatalf("reuse-Get returned cap %d, want 1024", got)
	}
	if got := len(*b2); got != 50 {
		t.Fatalf("reuse-Get returned len %d, want 50", got)
	}
	// shouldZero=false, so the mark must survive — proves it's the same backing array.
	if got := (*b2)[:1][0]; got != 0x42 {
		t.Fatalf("reuse-Get returned fresh buffer, want pooled (mark=0x42, got 0x%x)", got)
	}
	p.Put(b2)
}

func TestStrongRefSizedPool_ShouldZero(t *testing.T) {
	p := newStrongRefSizedBufferPool(64, true, 4)
	b := p.Get(64)
	(*b)[0] = 0x77
	(*b)[63] = 0xee
	p.Put(b)

	b2 := p.Get(64)
	if (*b2)[0] != 0 || (*b2)[63] != 0 {
		t.Fatalf("shouldZero=true should clear buffer; got [0]=0x%x [63]=0x%x", (*b2)[0], (*b2)[63])
	}
}

func TestStrongRefSizedPool_CapEnforced(t *testing.T) {
	p := newStrongRefSizedBufferPool(64, false, 3)
	// Put 5 buffers; only 3 should be retained.
	var bufs []*[]byte
	for i := 0; i < 5; i++ {
		b := make([]byte, 64)
		bufs = append(bufs, &b)
	}
	for _, b := range bufs {
		p.Put(b)
	}
	p.mu.Lock()
	got := len(p.free)
	p.mu.Unlock()
	if got != 3 {
		t.Fatalf("free list size = %d, want 3 (cap enforced)", got)
	}
}

func TestStrongRefSizedPool_WrongTierDropped(t *testing.T) {
	p := newStrongRefSizedBufferPool(1024, false, 4)
	// Putting a too-small buffer should be a no-op (matches sizedBufferPool semantics).
	tiny := make([]byte, 512)
	p.Put(&tiny)
	p.mu.Lock()
	got := len(p.free)
	p.mu.Unlock()
	if got != 0 {
		t.Fatalf("wrong-tier buffer was retained; free list size = %d, want 0", got)
	}
}

func TestStrongRefSizedPool_NilSafe(t *testing.T) {
	p := newStrongRefSizedBufferPool(1024, false, 4)
	p.Put(nil) // must not panic
}

func TestStrongRefSizedPool_ZeroCapDefault(t *testing.T) {
	p := newStrongRefSizedBufferPool(1024, false, 0)
	if p.capN != defaultStrongRefPoolCap {
		t.Fatalf("cap=0 should fall back to default; got %d, want %d", p.capN, defaultStrongRefPoolCap)
	}
}

func TestStrongRefSizedPool_ConcurrentStress(t *testing.T) {
	p := newStrongRefSizedBufferPool(4096, false, 256)
	const goroutines = 32
	const opsPerG = 500
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				b := p.Get(4096)
				if cap(*b) != 4096 {
					t.Errorf("concurrent Get returned cap %d, want 4096", cap(*b))
					return
				}
				(*b)[0] = byte(i)
				p.Put(b)
			}
		}()
	}
	wg.Wait()
}

func TestStrongRefSizedPool_SurvivesGC(t *testing.T) {
	// The core motivation for strong-ref pool: entries must survive
	// arbitrary GC cycles. Verify by Putting a buffer, forcing GC twice
	// (which would drain a sync.Pool entirely), and confirming the
	// buffer is still served from the pool.
	p := newStrongRefSizedBufferPool(2048, false, 8)
	b := p.Get(2048)
	(*b)[0] = 0xCD // unique marker
	p.Put(b)

	// Force two GC cycles. sync.Pool drains entirely on the second one.
	runtime.GC()
	runtime.GC()

	b2 := p.Get(2048)
	if got := (*b2)[:1][0]; got != 0xCD {
		t.Fatalf("buffer did not survive GC drain; want marker 0xCD, got 0x%x", got)
	}
}

func TestNewStrongRefDirtyBinaryTieredBufferPool_BasicFlow(t *testing.T) {
	p, err := NewStrongRefDirtyBinaryTieredBufferPool(12, 14, 20) // 4 KiB, 16 KiB, 1 MiB
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	// 4 KiB request → 4 KiB tier
	b := p.Get(4000)
	if got := cap(*b); got != 4096 {
		t.Fatalf("Get(4000) returned cap %d, want 4096 (4 KiB tier)", got)
	}
	(*b)[0] = 0xAB
	p.Put(b)

	b2 := p.Get(4000)
	if got := (*b2)[:1][0]; got != 0xAB {
		t.Fatalf("Get did not return pooled buffer; want 0xAB, got 0x%x", got)
	}
}

func TestNewStrongRefBinaryTieredBufferPool_ZeroesOnGet(t *testing.T) {
	p, err := NewStrongRefBinaryTieredBufferPool(12) // single tier
	if err != nil {
		t.Fatalf("constructor failed: %v", err)
	}
	b := p.Get(4096)
	(*b)[0] = 0xFF
	p.Put(b)

	b2 := p.Get(4096)
	if (*b2)[0] != 0 {
		t.Fatalf("zeroing variant should clear buffer; got [0]=0x%x", (*b2)[0])
	}
}

func TestStrongRefCapForTier(t *testing.T) {
	tests := []struct {
		size int
		want int
	}{
		// 16 MiB budget / size, clamped to [64, 8192].
		{size: 256, want: 8192},          // 16 MiB / 256 B = 65536, clamped to 8192
		{size: 4 * 1024, want: 4096},     // 16 MiB / 4 KiB = 4096 (hot SHM tier)
		{size: 16 * 1024, want: 1024},    // 16 MiB / 16 KiB = 1024
		{size: 32 * 1024, want: 512},     // 16 MiB / 32 KiB = 512
		{size: 128 * 1024, want: 128},    // 16 MiB / 128 KiB = 128
		{size: 512 * 1024, want: 64},     // 16 MiB / 512 KiB = 32, clamped to min 64
		{size: 1024 * 1024, want: 64},    // 16 MiB / 1 MiB = 16, clamped to min 64
		{size: 4 * 1024 * 1024, want: 64}, // big tier — floor at 64
		{size: 0, want: defaultStrongRefPoolCap},
		{size: -1, want: defaultStrongRefPoolCap},
	}
	for _, tc := range tests {
		got := strongRefCapForTier(tc.size)
		if got != tc.want {
			t.Errorf("strongRefCapForTier(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}
}

func TestStrongRefSizedPool_4KTierHoldsLargeWorkingSet(t *testing.T) {
	// Regression test for the SHM Send in-flight buffer problem: at
	// 1000-stream 4 KiB concurrent ping-pong the SHM transport holds
	// ~3500 buffers in flight (~20 µs/buffer × 172 K msg/s). The 4 KiB
	// tier cap of 4096 must accommodate this without alloc churn.
	const tier = 4 * 1024
	const inflight = 3500
	p := newStrongRefSizedBufferPool(tier, false, strongRefCapForTier(tier))
	bufs := make([]*[]byte, inflight)
	for i := 0; i < inflight; i++ {
		bufs[i] = p.Get(tier)
	}
	// Return them all. The pool should accept up to its cap and drop
	// the rest.
	for _, b := range bufs {
		p.Put(b)
	}
	p.mu.Lock()
	got := len(p.free)
	p.mu.Unlock()
	if got < 3500 {
		// On 4 KiB tier cap=4096 ≥ 3500, so all should be retained.
		t.Errorf("free list size = %d, want >= 3500 (4 KiB tier cap = %d)", got, strongRefCapForTier(tier))
	}
}
