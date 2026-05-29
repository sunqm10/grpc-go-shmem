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

// Temporary leak-hunt counters. NOT for production use; will be removed
// once the marshalWithPool buffer-leak attribution is identified.

package transport

import (
	"fmt"
	"sync/atomic"
)

// ShmLeakHuntCounters: 4 atomic counters wired at the suspect callsites
// for the marshalWithPool 79% buffer leak. Reset / dump from bench
// b.Cleanup via ResetShmLeakHuntCounters / DumpShmLeakHuntCounters.
//
// Expected invariants in steady-state bench:
//
//	WriterDRelease == EnqueueMsgRef
//	     (every data.Ref in enqueueMessageAndWait is balanced by
//	     exactly one d.release in the writer goroutine)
//
// If WriterDRelease < EnqueueMsgRef the writer is losing entries
// somewhere between trySend and the various d.release sites.
var (
	// Incremented once per `data.Ref()` in enqueueMessageAndWait
	// (whole-message channel path).
	shmLeakHuntEnqueueMsgRef uint64

	// Incremented once per `d.release()` in the writer goroutine
	// (advanceDeferred + processWholeMessage + close-drain paths).
	shmLeakHuntWriterDRelease uint64

	// Incremented when processWholeMessage's "misuse" / "zero-length"
	// short-circuit Frees data directly (no d allocated).
	shmLeakHuntWriterDirectFree uint64

	// Incremented when enqueueMessageAndWait rolls back data.Ref
	// because trySend failed (writer already closed).
	shmLeakHuntEnqueueRollback uint64

	// Incremented when tryInlineWrite returns handled=true (no Ref
	// taken, no writer Free needed — caller's Free brings refcount
	// to 0).
	shmLeakHuntTryInlineHandled uint64

	// ShmLeakHuntCallerFreeWired is incremented from upper-layer
	// (stream.go / server.go) right after the unconditional caller-
	// side Free of the channel-path marshal'd buffer. Exposed as a
	// public accessor so the upper layer (which cannot import
	// internal/transport directly via the package path used by
	// google.golang.org/grpc itself) can bump it without an import
	// cycle. Bumped via IncShmLeakHuntCallerFree.
	shmLeakHuntCallerFree uint64
)

// IncShmLeakHuntCallerFree is a public symbol used by stream.go's
// ZC fast path cleanup to record whether encData.Free was actually
// invoked. If shmLeakHuntCallerFree != marshalWithPool wraps, the
// caller-side cleanup is being skipped somehow.
func IncShmLeakHuntCallerFree() {
	atomic.AddUint64(&shmLeakHuntCallerFree, 1)
}

// ResetShmLeakHuntCounters zeros all counters.
func ResetShmLeakHuntCounters() {
	atomic.StoreUint64(&shmLeakHuntEnqueueMsgRef, 0)
	atomic.StoreUint64(&shmLeakHuntWriterDRelease, 0)
	atomic.StoreUint64(&shmLeakHuntWriterDirectFree, 0)
	atomic.StoreUint64(&shmLeakHuntEnqueueRollback, 0)
	atomic.StoreUint64(&shmLeakHuntTryInlineHandled, 0)
	atomic.StoreUint64(&shmLeakHuntCallerFree, 0)
}

// DumpShmLeakHuntCounters returns a human-readable snapshot.
func DumpShmLeakHuntCounters() string {
	ref := atomic.LoadUint64(&shmLeakHuntEnqueueMsgRef)
	rel := atomic.LoadUint64(&shmLeakHuntWriterDRelease)
	directFree := atomic.LoadUint64(&shmLeakHuntWriterDirectFree)
	rollback := atomic.LoadUint64(&shmLeakHuntEnqueueRollback)
	inline := atomic.LoadUint64(&shmLeakHuntTryInlineHandled)
	callerFree := atomic.LoadUint64(&shmLeakHuntCallerFree)
	return fmt.Sprintf(
		"shm leak-hunt counters:\n"+
			"  enqueueMessageAndWait Ref:     %12d  (channel-path data.Ref)\n"+
			"  enqueueMessageAndWait rollback:%12d  (trySend failed → data.Free rolled back)\n"+
			"  writer d.release:              %12d  (advanceDeferred + processWholeMessage + drain)\n"+
			"  writer direct Free:            %12d  (processWholeMessage misuse/zero-len)\n"+
			"  tryInlineWrite handled:        %12d  (no Ref, no writer Free — caller owns lifecycle)\n"+
			"  caller encData.Free (stream.go):%11d  (after withRetry — should match wraps if it fires)\n"+
			"  invariant: Ref - rollback == d.release\n"+
			"             diff = %d (>0 means writer is losing %d Frees)\n",
		ref, rollback, rel, directFree, inline, callerFree,
		int64(ref-rollback)-int64(rel),
		int64(ref-rollback)-int64(rel),
	)
}
