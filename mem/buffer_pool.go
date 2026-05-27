/*
 *
 * Copyright 2024 gRPC authors.
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
	"fmt"

	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/mem"
)

// BufferPool is a pool of buffers that can be shared and reused, resulting in
// decreased memory allocation.
type BufferPool interface {
	// Get returns a buffer with specified length from the pool.
	Get(length int) *[]byte

	// Put returns a buffer to the pool.
	//
	// The provided pointer must hold a prefix of the buffer obtained via
	// BufferPool.Get to ensure the buffer's entire capacity can be re-used.
	Put(*[]byte)
}

var (
	// defaultBufferPoolSizeExponents is the default tier set used by
	// DefaultBufferPool. Each entry is a log2 byte size; the pool
	// returns the smallest tier that fits a Get(size) request.
	//
	// Tiers 17 (128 KiB) and 19 (512 KiB) were added in this fork on
	// top of the upstream {8, 12, 14, 15, 20} set to close the 32 KiB →
	// 1 MiB gap that hurts mid-size message Marshal/MaterializeToBuffer
	// throughput. A 64 KiB proto Marshal previously landed in the 1 MiB
	// tier (16× over-allocation), causing memclr of 1 MiB on every
	// pool miss; with the 128 KiB tier the Marshal now allocates
	// 128 KiB (2× over-allocation). The change benefits all transports
	// that use this pool (TCP, UDS, SHM, in-process); SHM is the
	// largest beneficiary because its receive-side accumulator
	// (lpmAccumulator) also allocates per-LPM contiguous buffers
	// sized to the full message length.
	//
	// Tradeoff: each additional tier holds its own sync.Pool per P,
	// so memory headroom grows modestly with the number of tiers and
	// CPU count. For most workloads this is more than offset by the
	// reduced waste in the previously-overshot 32 KiB → 1 MiB gap.
	defaultBufferPoolSizeExponents = []uint8{
		8,
		12, // Go page size, 4KB
		14, // 16KB (max HTTP/2 frame size used by gRPC)
		15, // 32KB (default buffer size for io.Copy)
		17, // 128KB (fork-local: covers 32K+1..128K, e.g., 64KB Marshal + LPM hdr)
		19, // 512KB (fork-local: covers 128K+1..512K)
		20, // 1MB
	}
	defaultBufferPool BufferPool
)

func init() {
	var err error
	defaultBufferPool, err = NewBinaryTieredBufferPool(defaultBufferPoolSizeExponents...)
	if err != nil {
		panic(fmt.Sprintf("Failed to create default buffer pool: %v", err))
	}

	internal.SetDefaultBufferPool = func(pool BufferPool) {
		defaultBufferPool = pool
	}

	internal.SetBufferPoolingThresholdForTesting = func(threshold int) {
		bufferPoolingThreshold = threshold
	}
}

// DefaultBufferPool returns the current default buffer pool. It is a BufferPool
// created with NewBufferPool that uses a set of default sizes optimized for
// expected workflows.
func DefaultBufferPool() BufferPool {
	return defaultBufferPool
}

// NewTieredBufferPool returns a BufferPool implementation that uses multiple
// underlying pools of the given pool sizes.
func NewTieredBufferPool(poolSizes ...int) BufferPool {
	return mem.NewTieredBufferPool(poolSizes...)
}

// NewBinaryTieredBufferPool returns a BufferPool backed by multiple sub-pools.
// This structure enables O(1) lookup time for Get and Put operations.
//
// The arguments provided are the exponents for the buffer capacities (powers
// of 2), not the raw byte sizes. For example, to create a pool of 16KB buffers
// (2^14 bytes), pass 14 as the argument.
func NewBinaryTieredBufferPool(powerOfTwoExponents ...uint8) (BufferPool, error) {
	return mem.NewBinaryTieredBufferPool(powerOfTwoExponents...)
}

// NopBufferPool is a buffer pool that returns new buffers without pooling.
type NopBufferPool struct {
	mem.NopBufferPool
}

var _ BufferPool = NopBufferPool{}
