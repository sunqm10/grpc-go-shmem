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
	"os"
	"strconv"
	"sync/atomic"
)

// This file is the single source of truth for runtime tunables that the
// shared-memory transport reads from process environment variables.
// Centralising the reads here makes the configuration surface auditable
// for security and gRFC review, and gives downstream readers one place
// to look when they ask "what knobs does this transport expose at run
// time?".
//
// Design conventions
//
//   - Every env var read by SHM production code that influences
//     on-ring or on-wire behaviour MUST be declared and read in this
//     file. Pure diagnostic / debug-log knobs (GRPC_SHM_DEBUG,
//     GRPC_SHM_FUTEX_DEBUG) are read at the source where they gate
//     logging because they have no semantic effect on the transport;
//     they are listed in the Knobs section below for discoverability.
//     Test / benchmark scaffolding knobs (BENCH_PROFILE,
//     BENCH_DIRTY_DEFAULT_POOL, SHM_SPIN_ITERS, SHM_BENCH_CPU,
//     SHM_BENCH_ZC) are local to bench harness files and not part of
//     the production runtime API.
//
//   - Programmatic toggles preferred. Deployment-time mode switches
//     (eventfd waker, spin-then-block tuning) are exposed as
//     exported Configure* functions in the transport package rather
//     than env vars. This is the same pattern as
//     ConfigureShmSpinIterations / ConfigureShmFlowControlForBench.
//     Env vars are reserved for things that genuinely cannot be
//     expressed as in-process API: cross-process child identity
//     (set by the parent), and per-process diagnostic logging.
//
//   - Defaults are production-safe. A fresh process with none of these
//     set runs with the eventfd waker ON (Linux only; non-Linux uses
//     the futex / Windows-event fallback layer) and the HTTP/2-
//     compatible flow-control profile (the only profile in the
//     current transport).
//
// Knobs (alphabetical)
//
//   GRPC_CROSS_PROCESS_CHILD
//       Set to a non-empty value when the current process is the child
//       half of a cross-process SHM connection that was spawned via
//       experimental SCM_RIGHTS handoff. Disables eventfd-based wake
//       primitives that today still assume same-process file
//       descriptors. Used as a guard until cross-process FD passing
//       lands.
//
//   GRPC_SHM_DEBUG  (diagnostic only; read at ring.go init)
//       Non-empty enables verbose ring-buffer debug logging on
//       stderr. Has no effect on on-wire behaviour.
//
//   GRPC_SHM_FUTEX_DEBUG  (diagnostic only; read at
//                          shm_futex_{linux,windows}.go init)
//       Non-empty enables verbose futex syscall logging. Has no
//       effect on on-wire behaviour.
//
// Removed knobs (do not reintroduce without strong reason)
//
//   SHM_DATASEG_WAKE   - was the eventfd-waker opt-in. eventfd is now
//                        the default wake primitive on Linux (toggle
//                        for tests / bench: ConfigureShmEventfdWakerForBench).
//   SHM_NO_WU          - was the no-WINDOW_UPDATE opt-in. The NoWU
//                        mode has been REMOVED; HTTP/2-compatible flow
//                        control is now the only profile and NoWU-
//                        equivalent throughput is achieved by setting a
//                        large initial window (default 32 MiB).
//   SHM_INPROC_WAKE    - was a same-process bench-only wake registry;
//                        removed for being unrepresentative of real
//                        deployments.

// shmEnv is the package-private bag of env-derived booleans. All
// fields are evaluated exactly once at init time.
type shmEnv struct {
	crossProcessChild bool
}

var shmEnvFlags = readShmEnv()

func readShmEnv() shmEnv {
	return shmEnv{
		crossProcessChild: os.Getenv("GRPC_CROSS_PROCESS_CHILD") != "",
	}
}

// shmStdFlowOnlyAtomic gates the "standard-flow-only" profile (the
// cross-language–conformant extension posture). When set, the
// transport disables the two Go-private inline data-plane fast paths:
//
//   - writeProto's inline zero-copy direct-marshal-into-ring path
//     (client + server), and
//   - the frame writer's tryInlineWrite inline-under-inlineMu path.
//
// All message sends are instead routed through the writer goroutine's
// async queue, exactly as a conservative cross-language peer that does
// not implement the inline-coordination optimisation would observe.
// Reader-side WINDOW_UPDATE emission (enqueueOrInlineNonBlocking) is a
// cross-process-safe control-frame mechanism and is NOT affected. The
// negotiable jumbo frame size and large window are likewise unaffected —
// those are SETTINGS-expressible and remain available to the extension.
//
// Initialised once from GRPC_SHM_STD_FLOW at init; tests/benchmarks
// toggle it via ConfigureShmStdFlowOnlyForBench. Default OFF
// (production Go↔Go uses the inline fast paths). The env var is parsed
// with strconv.ParseBool, so GRPC_SHM_STD_FLOW=0 / false correctly
// leaves the mode disabled (a non-empty check would treat "0" as ON).
var shmStdFlowOnlyAtomic atomic.Bool

func init() {
	if v := os.Getenv("GRPC_SHM_STD_FLOW"); v != "" {
		if on, err := strconv.ParseBool(v); err == nil {
			shmStdFlowOnlyAtomic.Store(on)
		}
	}
}

// shmStdFlowOnly reports whether the standard-flow-only extension
// profile is active.
func shmStdFlowOnly() bool { return shmStdFlowOnlyAtomic.Load() }

// ConfigureShmStdFlowOnlyForBench toggles the standard-flow-only
// extension profile. Intended for benchmarks and tests that measure the
// cross-language extension posture against the Go-private fast paths.
func ConfigureShmStdFlowOnlyForBench(on bool) { shmStdFlowOnlyAtomic.Store(on) }
