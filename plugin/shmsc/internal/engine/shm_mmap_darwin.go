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

// Darwin twin of shm_mmap_unix.go (which, despite the name, is Linux-only:
// it uses fallocate(2)). Differences from Linux, all deliberate:
//
//   - Backing path: macOS has no /dev/shm, so segments live under
//     os.TempDir() directly — one candidate path, not two. Note that
//     os.TempDir() on macOS is per-user (and per login session), which is
//     fine for the same-user trust model this transport already requires
//     (0600 segment files), but means processes must share a session to
//     rendezvous by name. POSIX shm_open is NOT an option here: darwin caps
//     shm names at 31 bytes vs the engine's 200-byte segment names.
//   - Space reservation: fallocate(2) becomes fcntl(F_PREALLOCATE) followed
//     by ftruncate(2) — F_PREALLOCATE reserves blocks (failing up front with
//     ENOSPC instead of SIGBUS at first touch) but does not move EOF, so the
//     ftruncate sets the file size. A filesystem that does not support
//     preallocation (ENOTSUP) degrades to plain ftruncate, accepting the
//     (APFS-rare) sparse-file risk the Linux code eliminates on tmpfs.
//
// mmap/munmap themselves are identical POSIX calls on darwin.

package engine

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func init() {
	// Set platform-specific function implementations
	unmapMemory = munmapImpl
}

// CreateSegment creates a new shared memory segment for the server
func CreateSegment(name string, ringCapA, ringCapB uint64) (*Segment, error) {
	// Calculate the layout
	totalSize, ringAOffset, ringBOffset, err := CalculateSegmentLayout(ringCapA, ringCapB)
	if err != nil {
		return nil, fmt.Errorf("layout calculation failed: %w", err)
	}

	path := generateSegmentPath(name)

	// Create the file. A pre-existing file is an error, never a fallback:
	// unlinking it could strand a live listener on an orphaned inode.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("segment %q already exists: %w", name, err)
		}
		return nil, fmt.Errorf("failed to create segment file %s: %w", path, err)
	}

	// Reserve the blocks up front (the fallocate analog), then set the file
	// size. F_PREALLOCATE surfaces ENOSPC here rather than as a SIGBUS at
	// first touch; ENOTSUP (non-APFS/HFS filesystems) degrades to a plain
	// ftruncate.
	fd := file.Fd()
	fstore := unix.Fstore_t{
		Flags:   unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  int64(totalSize),
	}
	if err := unix.FcntlFstore(fd, unix.F_PREALLOCATE, &fstore); err != nil && !errors.Is(err, unix.ENOTSUP) {
		file.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to create segment in any location: preallocate failed: %w", err)
	}
	if err := unix.Ftruncate(int(fd), int64(totalSize)); err != nil {
		file.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to create segment in any location: ftruncate failed: %w", err)
	}

	// Memory map the file
	mem, err := mmapFile(file, int(totalSize))
	if err != nil {
		file.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to create segment in any location: %w", err)
	}

	segment := &Segment{
		File: file,
		Mem:  mem,
		Path: path,
		H:    &hdrView{basePtr: unsafe.Pointer(&mem[0])},
		A:    &ringView{basePtr: unsafe.Pointer(&mem[0]), offset: ringAOffset},
		B:    &ringView{basePtr: unsafe.Pointer(&mem[0]), offset: ringBOffset},
	}

	// Initialize the segment header
	magic := [8]byte{'G', 'R', 'P', 'C', 'S', 'H', 'M', 0}
	segment.H.SetMagic(magic)
	segment.H.SetVersion(SegmentVersion)
	segment.H.SetTotalSize(totalSize)
	segment.H.SetRingAOffset(ringAOffset)
	segment.H.SetRingACapacity(ringCapA)
	segment.H.SetRingBOffset(ringBOffset)
	segment.H.SetRingBCapacity(ringCapB)
	segment.H.SetServerPID(uint32(os.Getpid()))
	segment.H.SetMaxStreams(math.MaxUint32)

	// Initialize ring headers
	segment.A.SetCapacity(ringCapA)
	segment.A.SetWriteIndex(0)
	segment.A.SetReadIndex(0)
	segment.A.SetClosed(false)

	segment.B.SetCapacity(ringCapB)
	segment.B.SetWriteIndex(0)
	segment.B.SetReadIndex(0)
	segment.B.SetClosed(false)

	// No-op on darwin: the eventfd waker is Linux-only, so both sides
	// publish OpenerWakeReady=false and converge on the polling futex path.
	setupDataSegWakeForCreator(segment)

	// Close the backing file fd: the mmap holds an independent inode
	// reference, so the mapped region stays valid for the segment's
	// lifetime. Path is preserved in segment.Path for path-based unlink via
	// RemoveSegment / Segment.Close.
	if err := file.Close(); err != nil {
		munmapImpl(mem)
		os.Remove(path)
		return nil, fmt.Errorf("failed to create segment in any location: close fd after mmap: %w", err)
	}
	segment.File = nil

	return segment, nil
}

// OpenSegment opens an existing shared memory segment for the client
func OpenSegment(name string) (*Segment, error) {
	// Generate the segment path (single candidate on darwin).
	path := generateSegmentPath(name)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file %s: %w", path, err)
	}

	// Get file info to determine size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat segment file: %w", err)
	}

	size := info.Size()
	if size < SegmentHeaderSize {
		file.Close()
		return nil, fmt.Errorf("segment file too small: %d bytes", size)
	}

	// Memory map the file
	mem, err := mmapFile(file, int(size))
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap segment: %w", err)
	}

	// Create header view for validation
	hdr := &hdrView{basePtr: unsafe.Pointer(&mem[0])}

	// Validate the header
	if err := ValidateSegmentHeader((*SegmentHeader)(hdr.basePtr)); err != nil {
		munmapImpl(mem)
		file.Close()
		return nil, fmt.Errorf("invalid segment header: %w", err)
	}

	// Get ring offsets from header
	ringAOffset := hdr.RingAOffset()
	ringBOffset := hdr.RingBOffset()

	// Create segment views
	segment := &Segment{
		File: file,
		Mem:  mem,
		Path: path,
		H:    hdr,
		A:    &ringView{basePtr: unsafe.Pointer(&mem[0]), offset: ringAOffset},
		B:    &ringView{basePtr: unsafe.Pointer(&mem[0]), offset: ringBOffset},
	}

	// Set client PID. SetClientReady is deferred until AFTER
	// setupDataSegWakeForOpener has recorded OpenerWakeReady in the header,
	// so the creator's WaitForClient gate releases with a stable wake-mode
	// flag (see shm_mmap_unix.go for the full rationale). On darwin the
	// setup is a no-op that publishes OpenerWakeReady=false.
	segment.H.SetClientPID(uint32(os.Getpid()))
	setupDataSegWakeForOpener(segment)
	segment.H.SetClientReady(true)

	// Close the backing file fd: the mmap holds an independent inode
	// reference. See CreateSegment.
	if err := file.Close(); err != nil {
		munmapImpl(mem)
		return nil, fmt.Errorf("close fd after mmap: %w", err)
	}
	segment.File = nil

	return segment, nil
}

// generateSegmentPath generates the file path for a shared memory segment.
// Darwin has no /dev/shm; segments live under the (per-user) temp dir.
func generateSegmentPath(name string) string {
	return filepath.Join(os.TempDir(), "grpc_shm_"+name)
}

// mmapFile memory maps a file
func mmapFile(file *os.File, size int) ([]byte, error) {
	fd := int(file.Fd())

	data, err := syscall.Mmap(fd, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	return data, nil
}

// munmapImpl unmaps a memory-mapped region
func munmapImpl(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	err := syscall.Munmap(data)
	if err != nil {
		return fmt.Errorf("munmap failed: %w", err)
	}

	return nil
}
