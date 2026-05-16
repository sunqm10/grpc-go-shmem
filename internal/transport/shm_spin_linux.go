//go:build linux

/*
 *
 * Copyright 2025 gRPC authors.
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

// Linux spin-wait constants. The reader spins for up to dataSpinCutoff
// iterations of runtime_procyield(1) (≈7ns each) before incrementing the
// waiter count and calling futex(WAIT). If data arrives within the spin
// window, the wait is skipped entirely — and so is the corresponding
// futex(WAKE) on the writer side (it observes waiters == 0 and elides
// the syscall). For ping-pong RPCs this is the single biggest latency
// knob: every wake we skip saves ~30–80 µs of OS-thread reschedule
// overhead (futex syscall + runtime.startm + cache reload), which the
// CPU profile of BenchmarkGRPCShmStream/size=64 attributes ~46 % of
// total time to.
//
// The values below are tuned for HW where runtime_procyield(1) costs
// ~7 ns and one full futex wake/wait round-trip costs ~25–50 µs.
// The adaptive logic in ring.go grows the per-ring cutoff toward
// spinIterationsMax on successful spins and shrinks toward
// spinIterationsMin on misses; the floor must stay high enough that
// a ping-pong workload (where every wait misses the spin window once)
// doesn't collapse to a level too low to ever catch the next round.
const (
	// spinIterationsDefault: starting cutoff for a fresh ring.
	// ~28 µs covers a same-process write→signalData→wake path on
	// a quiescent CPU and matches the typical inter-frame gap of
	// small-to-medium streaming workloads.
	spinIterationsDefault = 4000

	// spinIterationsMin: adaptive floor. ~14 µs — keeps the floor
	// high enough that an alternating ping-pong workload still
	// catches occasional fast responses without a futex round-trip.
	// Below ~5 µs (700 iters) the reader essentially never wins
	// the race and pays both the spin cost AND the futex cost.
	spinIterationsMin = 2000

	// spinIterationsMax: cap for sustained throughput workloads.
	// ~225 µs — large enough to absorb the full handler+send path
	// of a small-message ping-pong (typical 30–150 µs on a busy
	// box) and trade a fraction of a core's CPU for elimination
	// of one of the four wake-ups per iteration. Far below the
	// scheduler quantum so a runnable peer never gets starved.
	spinIterationsMax = 32000
)
