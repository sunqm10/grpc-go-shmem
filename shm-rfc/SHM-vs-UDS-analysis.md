# SHM vs UDS: Attribution of Performance Difference

**Purpose.** Address the question of whether the gRPC-Go Unix-domain-socket
(UDS) path could be optimized to approach in-memory-transport (SHM)
performance. The data suggests UDS cannot be made to match SHM within
gRPC-Go's current `net.Conn` / HTTP/2 transport path alone. The remaining
UDS-specific opportunity appears to be below gRPC-Go's transport
abstraction, in the kernel and in the OS-interface boundary itself.

**Setup.** Azure Linux VM, Intel Xeon Platinum 8370C @ 2.80 GHz, 16 vCPU;
Go 1.25; `BENCH_PROFILE=fair-default` (HTTP/2 frame = 16 KB, initial
stream/conn windows = 65,535 B — symmetric for both transports; no
SHM-favouring knobs). §1 and §2.1–§2.4 use 3-run medians; §2.5
`/proc/<pid>/io` deltas are a single 9-second steady-state window
(disclosed there). Raw artifacts available on request.

---

## 1. Headline data

### 1.1 Latency and per-RPC CPU

| Cell | SHM ns/op | UDS ns/op | **Perf gain** | SHM cpu-ns/op | UDS cpu-ns/op | **UDS extra CPU** |
|---|---|---|---|---|---|---|
| Unary 1 KB | 49,628 | 69,585 | 1.40× | 790 K | 1,111 K | +41 % |
| Stream 64 KB | 141,853 | 222,946 | 1.57× | 2,269 K | 3,567 K | +57 % |
| Concurrent 100 streams × 4 KB | 690,703 | 1,011,562 | 1.46× | 11,049 K | 16,182 K | +46 % |
| **Concurrent 1000 streams × 4 KB** | **5,542,612** | **9,929,132** | **1.79×** | **88,971 K** | **159,347 K** | **+79 %** |

The latency gain and CPU saving move together and at the same scale —
SHM's 1.4-1.8× latency improvement corresponds to a 0.56-0.71× CPU
footprint. SHM is not buying speed with extra CPU.

The high-concurrency cell shows the gap widens under load (Unary +41 % →
1000 streams +79 %), consistent with syscall overhead accumulating per
in-flight RPC.

### 1.2 Kernel time per RPC

System time reported by `/usr/bin/time -v`, divided by iterations:

| Cell | SHM kernel µs/RPC | UDS kernel µs/RPC | **UDS extra** |
|---|---|---|---|
| Unary 1 KB | ~16 | ~21 | +31 % |
| Stream 64 KB | ~77 | ~151 | **+96 %** |
| Concurrent 100/4K (per stream-event) | ~6.6 | ~12.2 | **+85 %** |

UDS spends 1.3-2× the CPU inside the kernel relative to SHM. This time
is consumed by `sys_read` / `sys_write` / `epoll_pwait` handlers — code
that runs inside the kernel, not gRPC-Go, and that gRPC-Go cannot reach.

### 1.3 Where flat CPU lands

`go tool pprof -top -flat` percentage attributable to the two hottest
kernel-entry functions (`internal/runtime/syscall.Syscall6` and
`runtime.futex`):

| Cell | SHM (Syscall6 + futex) | UDS (Syscall6 + futex) |
|---|---|---|
| Unary 1 KB | 22.6 % | 28.8 % |
| Stream 64 KB | 23.7 % | **35.2 %** |
| Concurrent 100/4K | 5.2 % | **13.8 %** |

UDS spends a materially larger share of its cycles at the kernel
boundary, especially in the stream and concurrent cells. Under SHM these
entries are dominated by the data-segment wake mechanism (eventfd on
Linux by default in this build, with `runtime.futex` for Go's scheduler
and mutex parking); under UDS the same flat samples include
`read` / `write` / `epoll_pwait` for the socket data path on top of
Go's scheduler futex traffic.

---

## 2. Hot-path comparison

To make "where the extra cycles go" concrete, here is the minimal set of
operations each transport performs for a single unary RPC. Both paths
run the full gRPC API and the same HTTP/2-compatible semantics under the
same frame and window profile, and share the codec, HPACK encoder, and
stream state machine. The SHM transport adds a DATA-frame fast path that
marshals proto bytes directly into the ring (it is selectable through
gRPC's `WriteProto` extension point; the stock HTTP/2 client does not
use it). The differences highlighted below are at the transport boundary
and in the DATA emission path; everything above is shared.

### 2.1 UDS, one unary RPC

Client send:
1. `proto.Marshal` → `mem.BufferSlice` (user-space alloc + memmove).
2. Wake loopyWriter goroutine (chan send → gopark/goready).
3. HTTP/2 framer encodes HPACK headers.
4. HTTP/2 framer writes a DATA frame into an internal buffer.
5. **`syscall.write(uds_fd, buf)`** — first socket syscall.
6. Kernel enters the socket write path (`sock_write_iter` →
   `sock_sendmsg` → `unix_stream_sendmsg`) → **copy_from_user** into a
   kernel skb → enqueue on peer's `sk_receive_queue` → wake peer's epoll.

Server receive:
7. Server netpoll goroutine returns from `epoll_pwait`.
8. **`syscall.read(uds_fd, buf)`** — second socket syscall → **copy_to_user**
   from kernel skb.
9. HTTP/2 framer parses HPACK + DATA.
10. `proto.Unmarshal` (memmove into message fields).
11. User handler runs; the response retraces steps 1–10 in reverse.

For an isolated unary RPC this is typically **2 write-side and 2
read-side socket syscalls** (counts can vary with HTTP/2 frame
coalescing, bufio flushes, WINDOW_UPDATE handling and `epoll_pwait`
batching), plus the corresponding **2 kernel↔user buffer copies** per
direction, and **intermediate user-space buffer allocations** before
each write.

### 2.2 SHM, one unary RPC

Client send:
1. `proto.MarshalAppend` writes **directly into ring memory** (no
   intermediate `mem.BufferSlice`).
2. Wake the server reader via the data-segment wake primitive (eventfd
   write on Linux in the bench build).

Server receive:
3. Server reader goroutine returns from the data-segment wait (eventfd
   read).
4. Take proto bytes out of the ring → `proto.Unmarshal` (one memmove,
   same size as UDS).
5. User handler runs; the response retraces steps 1–4 in reverse.

Per unary RPC: **0 socket syscalls**, **2 data-segment wake/wait pairs**
(eventfd in this build, futex in alternative builds — both are kernel
transitions, just much cheaper than `sendmsg`+`epoll_pwait` round-trips),
**0 kernel socket-buffer copies**, **0 intermediate allocations**.

### 2.3 Difference matrix

| Item | UDS | SHM | Why it differs |
|---|---|---|---|
| DATA marshal destination | intermediate `mem.BufferSlice` | directly into ring (on the eligible `WriteProto` inline path; chunked fallback when payload > `shmMaxFrameSize` or under contention) | UDS forces an extra alloc + memmove on every DATA write because `net.Conn.Write([]byte)` requires the bytes to exist first. SHM keeps the option to skip it when inline-eligible. |
| HPACK headers/trailers | yes | yes | Identical cost — shared gRPC-Go code on both paths. |
| DATA write handoff | always via loopyWriter chan | bypassed on the inline `WriteProto` fast path; uses the SHM writer goroutine on the chunked/fallback path | Architectural difference at the transport interface. |
| Wake / wait primitive | `epoll_pwait` + `read`/`write` on socket fd | data-segment eventfd (or futex) | A socket round-trip is meaningfully more work than a single eventfd / futex transition. |
| Kernel buffer copy | 2× (user→kernel→user) | 0 | UDS spends an extra ~2× payload-size memory-bandwidth per RPC. |
| Socket syscalls / RPC | typically 4 | 0 | Physical kernel-entry overhead UDS cannot avoid. |

The profiles do not show an obvious UDS-only gRPC-Go userspace hotspot
large enough to explain the measured gap. The universal SHM advantage
is (a) bypassing the AF_UNIX socket data path entirely; on top of
that, eligible `WriteProto` writes additionally (b) marshal proto bytes
directly into the ring. The fair-default 64 KB stream cell exercises
the chunked fallback (payload exceeds the 16 KB SHM max frame), so
its gap is dominated by (a) rather than (b). Neither (a) nor (b) is
reachable while preserving the `net.Conn` abstraction that UDS sits
on.

### 2.4 Block-profile shape

`go tool pprof -sample_index=delay` for the unary 1 KB cell shows what
goroutines are waiting on:

| Function | SHM | UDS |
|---|---|---|
| `runtime.selectgo` | 51.2 % | 38.6 % |
| `runtime.chanrecv2` | 29.0 % | 36.8 % |
| `runtime.chanrecv1` | 19.7 % | 24.6 % |

The top wait sites are the same on both paths — both transports park
inside gRPC-Go's `recvBuffer` channel and `controlBuf`. The block
profile does not by itself quantify the cost gap (delay% is park-time,
not CPU attribution), but it does not reveal a distinct UDS-only
userspace blocking site that SHM avoids. Whatever extra cost UDS pays
is showing up as more time inside the kernel between two structurally
identical wait points, not as a different gRPC-Go waiting topology.

### 2.5 Buffer copy attribution

Buffer copy work splits into three categories: (i) kernel-side socket
buffer copies (`copy_to_user` / `copy_from_user` inside socket syscall
handlers), (ii) user-space protobuf marshal / unmarshal, and (iii)
user-space transport-internal copies (framer staging buffers, per-frame
accumulator, `mem.BufferSlice.CopyTo` style merges). This section
attributes each category from `/proc/<pid>/io` (kernel) and
`go tool pprof -peek runtime.memmove` (user-space caller breakdown).

The benchmark VM has BPF and `perf_event_open`-based syscall tracing
disabled by kernel lockdown (Azure Trusted Launch / Secure Boot
integrity mode), so byte-accurate kernel attribution is taken from
`/proc/<pid>/io` instead. `rchar` / `wchar` are process-level logical
bytes returned by read-like and accepted by write-like fd operations.
In this benchmark that includes the AF_UNIX socket reads/writes (UDS
cells) and the data-segment eventfd wake reads/writes (SHM cells).
`read_bytes` / `write_bytes` ≈ 0 rules out block-device I/O as the
source. Reported values are deltas within a 9-second steady-state
window, divided by the RPC count attributable to that window.

#### 2.5.1 Kernel-side bytes per RPC

All cells are 64 KB streaming (`BenchmarkGRPCShmStream/size=65536` /
`BenchmarkGRPCUnixStream/size=65536`). The `fair_*` cells use
`BENCH_PROFILE=fair-default`; the `jumbo_shm` cell additionally sets
`SHM_MAX_FRAME_SIZE=1048576` to raise the SHM transport's max DATA
frame to 1 MiB.

| Cell | ns/op | rchar B/RPC | wchar B/RPC | **kernel bytes/RPC** |
|---|---|---|---|---|
| fair_shm (SHM frame = 16 K) | 139,216 | 57 | 115 | **172** |
| **fair_uds (HTTP/2 frame = 16 K)** | 224,294 | 109,077 | 109,077 | **218,154** |
| jumbo_shm (SHM frame = 1 M) | 98,263 | 20 | 24 | **44** |

The `jumbo_shm` row uses the same UDS baseline as `fair_uds`; it is
included to show that SHM's already-small kernel byte footprint
shrinks further when the SHM transport's single-frame ZC fast path is
engaged. The `SHM_MAX_FRAME_SIZE` knob applies to the SHM producer
only — the UDS path stays on grpc-go's default 16 KiB HTTP/2 DATA
frame size regardless of profile, so there is no corresponding
"jumbo_uds" to report. Structurally, UDS byte traffic is
payload-dominated: every payload byte traverses the AF_UNIX socket
buffer regardless of window or frame size.

Per 64 KB RPC, UDS pushes ~218 KB through the kernel socket buffer.
Client and server goroutines run in the same process in this bench,
so `/proc/<pid>/io` captures both endpoints — each payload byte is
counted once as `wchar` on the sending side and once as `rchar` on
the peer's receive, yielding symmetric ~109 KB each direction
(measured 218 KB total vs. a naive 2×(req + resp) ≈ 262 KB upper
bound; the difference is partial reads, control frames, and
bookkeeping).

SHM moves only ~170 bytes per RPC at fair_shm. The SHM TX side does
~14.2 eventfd writes/RPC (`ds-wake-sys/op = 14.23`) at 8 bytes each,
so `wchar` should be ≈ 114 B and matches the observed 115. `rchar`
is lower (57) because eventfd reads coalesce multiple pending wakes
into one 8-byte counter read on the RX side. At jumbo_shm the
per-RPC wake count drops to ~3 (single-frame ZC fast path) and the
wake byte traffic falls to ~44 B.

The measured ratio of UDS to SHM kernel logical fd bytes is ~1.3K×
at fair_shm (218,154 / 172) and ~4.9K× at jumbo_shm (218,154 / 44).
This is a ratio of logical fd byte traffic, not CPU; the corresponding
CPU impact is captured by §1.1 and §1.2.

#### 2.5.2 User-space memmove attribution by caller

`go tool pprof -peek runtime.memmove` lists the functions that
immediately call `runtime.memmove`, weighted by sample time. Across
all four cells, the contributors group cleanly into two categories:
**proto marshal/unmarshal** (`protowire.AppendBytes` for marshal,
`impl.consumeBytesNoZero` for unmarshal of the `bytes` payload field)
and **transport / adapter internal copy** (everything else: SHM ring
writer / framer / accumulator, or UDS `bufWriter` / `bufReadyReader` /
`mem.BufferSlice.CopyTo` materialization).

Numbers below are aggregate pprof CPU sample microseconds per
benchmark op (i.e. one Send+Recv ping-pong = 1 iter), computed as the
caller's cumulative memmove time divided by the cell's iteration
count. The unit is CPU time, not wall time.

| Cell | iter | **proto memmove µs/RPC** | **transport memmove µs/RPC** | transport caller breakdown |
|---|---|---|---|---|
| fair_shm  |  85 K | 26 | **34** | `ringSegWriter.write` 18 + `readFrameViewH2` 13 + `lpmAccumulator.feedSplit` 3 |
| fair_uds  |  53 K | 22 | **24** | `bufWriter.Write` 8 + `bufReadyReader.Read` 7 + `BufferSlice.CopyTo` 9 |
| jumbo_shm | 115 K | 48 | **0.4** | `readFrameViewH2` 0.4; rest eliminated |
| jumbo_uds |  68 K | 22 | **21** | `bufWriter.Write` 5 + `bufReadyReader.Read` 8 + `BufferSlice.CopyTo` 8 |

Three observations from this table.

1. **Proto marshal/unmarshal is paid on both transports** — it is
   not a UDS-specific opportunity. Three of the four cells land in a
   narrow band (22-26 µs CPU/RPC); jumbo_shm comes in higher at
   48 µs. The most plausible mechanism is destination/source cache
   locality on the ZC fast path: `protoMarshalAppend` writes the
   64 KB payload **directly into a fresh ring position**
   ([writeProtoToRingH2Core](../internal/transport/h2_codec.go#L2875)
   marshals into `res.First`, the reserved ring slice) which has
   little temporal reuse and likely cold L1/L2 residency; the
   matching ZC unmarshal on the receive side reads from the same
   ring-backed bytes. The other three cells marshal into / unmarshal
   from a reused Go heap pool buffer that stays warm in cache,
   making the same `protowire.AppendBytes` /
   `impl.consumeBytesNoZero` work run roughly 2× faster per byte.
   This per-byte slowdown is a tradeoff for eliminating a separate
   transport-internal copy (heap-to-ring on `fair_shm`, or
   heap-to-bufWriter on the UDS cells) — the net user-space byte
   movement is summarized in §2.5.3 below. Either way this column
   is bounded by the cost of generated proto code and is not
   addressable from the transport layer.

2. **Transport-internal memmove is where the structural difference
   lies.** UDS pays ~21-24 µs/RPC on `bufWriter` + `bufReadyReader`
   + `BufferSlice.CopyTo` in every cell, because these copies are
   structural to grpc-go's HTTP/2 framer over a `net.Conn`: the
   framer must marshal HTTP/2 frames into a contiguous buffer before
   `Write([]byte)`, and the reader must accumulate incoming bytes
   into a `BufferSlice` for the codec. grpc-go's write/read buffer
   knobs ([`WithWriteBufferSize`](https://pkg.go.dev/google.golang.org/grpc#WithWriteBufferSize),
   [`WriteBufferSize`](https://pkg.go.dev/google.golang.org/grpc#WriteBufferSize)
   server option, and the read counterparts) can move this cost
   around — e.g. zero-buffer modes shift it to more underlying
   syscalls instead of fewer larger ones — but they do not give UDS
   the equivalent of SHM's direct-ring path. **`jumbo_shm` reduces
   transport-internal memmove to 0.4 µs/RPC** — ~50× lower than UDS
   — by routing the entire 64 KB payload through the single-frame
   ZC fast path: the producer marshals directly into ring memory and
   the consumer parses directly out of it, without an intermediate
   copy.

3. **`fair_shm` is the only cell where SHM transport memmove is
   higher than UDS** (34 µs vs 24 µs, a 10 µs/RPC gap). This
   comes from the SHM chunked path: under fair-default's 16 KB max
   frame, a 64 KB LPM splits into 4-5 DATA frames, each one copied
   by `ringSegWriter.write` on TX and recombined by `feedSplit` on
   RX. It is a cost of the chunked fallback path implementation,
   not of shared-memory architecture, and it is removed by the
   configuration switch that enables the ZC fast path. The fair-default
   profile is included here as a conservative apples-to-apples baseline
   against UDS; the SHM transport's default-tuned and production
   profiles use a larger max frame so that mid-size payloads stay on
   the ZC fast path (this note's `jumbo_shm` cell uses
   `SHM_MAX_FRAME_SIZE=1048576` explicitly to make the comparison
   reproducible).

#### 2.5.3 Combined

| | fair_shm | fair_uds | jumbo_shm | jumbo_uds |
|---|---|---|---|---|
| Kernel bytes / RPC | **172** | 218,154 | **44** | 215,103 |
| Proto memmove µs/RPC † | 26 | 22 | 48 | 22 |
| Transport memmove µs/RPC | 34 | 24 | **0.4** | 21 |
| Combined latency (ns/op) | **139 K** | 224 K | **98 K** | 174 K |

† Proto cost is structurally paid on both transports and is not a
UDS-vs-SHM differentiator. jumbo_shm's higher absolute value reflects
the cold-ring vs warm-heap cache-locality tradeoff described in
§2.5.2 obs. 1. The single-stream ping-pong bench is a near-best
case for warm-heap reuse on UDS; production workloads with
concurrent streams, GC pressure, and handler work that evicts the
heap pool would erode UDS's warm-cache advantage while leaving SHM's
cold-ring locality tradeoff largely intact, so the 48 vs 22 µs
proto-memmove gap measured here is likely the worst case for SHM
rather than the typical case.

Dominant transport-side payload writes per 64 KB message direction
(excluding kernel socket copies handled by §2.5.1 and the unmarshal
copy paid by every cell):

| Cell | proto write dst | transport copies | **transport-side payload writes** |
|---|---|---|---|
| fair_shm  | heap pool (warm) | heap-to-ring as 4-5 chunks, plus RX accumulate | ~3× 64 KB direction |
| fair_uds  | heap pool (warm) | heap-to-framer + bufWriter, plus RX read-side copy | ~2× 64 KB direction |
| **jumbo_shm** | **ring (cold)** | none on hot path; RX reads ring directly | **~1× 64 KB direction** |
| jumbo_uds | heap pool (warm) | heap-to-framer + bufWriter, plus RX read-side copy | ~2× 64 KB direction |

Takeaways:

- **Kernel-side**: SHM's logical fd byte traffic is 1,300-5,000×
  below UDS in every profile. UDS cannot close this gap from inside
  grpc-go regardless of configuration.
- **User-space transport-internal**: UDS's ~22 µs/RPC is grpc-go
  HTTP/2 framer/adapter overhead bound to the `net.Conn` interface;
  buffer knobs can move it around but cannot give UDS a zero-copy
  path. `jumbo_shm` collapses this to 0.4 µs (~50× lower).
  `fair_shm`'s extra 10 µs is an SHM chunked-path implementation
  cost, eliminated by the jumbo configuration.
- **User-space proto**: paid on both transports and not a
  differentiator; jumbo_shm's higher reading is a bench-cache
  artifact that likely overstates the proto-side penalty against
  SHM in this single-stream benchmark (see footnote above).

End-to-end, SHM's jumbo profile ships at 98 K ns/op vs UDS's 174 K
ns/op — a 76 µs/RPC absolute saving consistent with the combined
kernel + transport copy reductions traced above.

---

## 3. UDS-specific opportunity lives below gRPC-Go

The data points in the same direction:

1. The block profile does not reveal a UDS-specific gRPC-Go userspace
   blocking site; both transports park at the same channel and select
   points.

2. The flat-CPU gap concentrates in `Syscall6` and `runtime.futex` —
   kernel-entry instructions, not gRPC-Go userspace code. Caller-level
   attribution of `runtime.memmove` (§2.5.2) shows protobuf
   marshal/unmarshal is paid by both transports and is not a
   UDS-specific opportunity; the residual transport-internal memmove
   on UDS (~22 µs/RPC, from grpc-go's HTTP/2 framer / adapter
   buffers) is not net-eliminated by known grpc-go HTTP/2 buffer
   tuning, while SHM's chunked-path overhead at fair-default is
   removed by the jumbo configuration that takes its
   transport-internal memmove to ~0.4 µs/RPC.

Kernel-adjacent mechanisms that might in principle reduce the UDS gap
are not available through gRPC-Go's current UDS `net.Conn` path:
`MSG_ZEROCOPY` is documented for TCP / UDP / VSOCK and is not part of
the AF_UNIX stream socket interface; `io_uring` would require
integration into either the Go runtime netpoller or a different
transport adapter and would still not remove AF_UNIX kernel copies;
`splice` requires a pipe endpoint and does not help an HTTP/2 transport
that needs framed in-process access to bytes; `sendmmsg` does not match
the synchronous request/response shape of gRPC RPCs. None of these is
addressable by a code change inside `google.golang.org/grpc`.

As a coarse lower-bound: a clean isolated unary RPC carries on the
order of four socket syscalls (two each side). At Linux best-case
unloaded overhead of ~1-2 µs per socket syscall, that is a ~4-8 µs
floor — roughly 20-40 % of the ~20 µs unary gap we measure today. The
remainder includes AF_UNIX kernel copies, queue/wake behaviour,
scheduling effects, and the shared gRPC-Go upper-layer code that both
transports execute. None of these is reachable by improving gRPC-Go's
UDS userspace path alone.

---

## 4. Conclusions

1. UDS uses materially more CPU per RPC than SHM: +41 % at unary 1 KB
   rising to +79 % at 1000 concurrent streams × 4 KB. The figure matches
   the informal "≈ 2× perf ≈ 0.5× CPU" expectation.

2. The extra cycles are not attributable to a specific gRPC-Go userspace
   hotspot. Block profiles show the same waiting shape on both paths;
   flat-CPU differences concentrate in `Syscall6` and `runtime.futex`,
   i.e. at the kernel boundary. `/proc/<pid>/io` confirms this
   directly: in the 64 KB stream cell, UDS moves ~218 KB/RPC through
   fd reads/writes versus SHM's ~44-172 B/RPC of eventfd wake traffic
   (~1,300-5,000× ratio; see §2.5.1). On the user-space side,
   caller-attributed `runtime.memmove` shows ~22 µs/RPC of
   transport-internal copy on UDS that the SHM jumbo ZC fast path
   collapses to 0.4 µs (~50× lower; §2.5.2).

3. The remaining UDS-specific opportunity is below gRPC-Go's transport
   abstraction: kernel mechanisms such as `MSG_ZEROCOPY`, `io_uring`,
   `splice` are not available through gRPC-Go's current UDS
   `net.Conn` path (unsupported for `AF_UNIX` stream sockets, requiring
   Go-runtime changes, or incompatible with gRPC's synchronous RPC
   semantics).

4. SHM's end-to-end advantage composes from two structurally
   independent mechanisms, both visible in the data:

   - **(a) Bypassing the kernel socket path.** Replaces UDS's
     ~4 socket syscalls per RPC + ~218 KB of kernel `copy_to_user`
     / `copy_from_user` per 64 KB RPC with eventfd wake traffic only
     (measured 44-172 B/RPC in this note's cells; ~3 wakes/RPC under
     jumbo, ~14 under fair-default). This drives the §1.2 / §1.3
     kernel-time and `Syscall6`-flat reductions and the §2.5.1
     byte-traffic elimination.
   - **(b) Eliminating user-space transport-internal copies.** On
     the inline ZC fast path, the producer marshals proto bytes
     directly into ring memory and the consumer parses them in
     place — cutting grpc-go HTTP/2's ~22 µs/RPC of
     `bufWriter` / `bufReadyReader` / `BufferSlice.CopyTo` work to
     ~0.4 µs (§2.5.2 obs. 2). This path covers the 1 KB unary and
     4 KB concurrent cells under any profile, and the 64 KB stream
     cell under the jumbo (1 M SHM frame) profile. The fair-default
     64 KB stream case falls back to a chunked path that adds
     ~10 µs/RPC of SHM-side copy; production deployments use a
     large-frame profile to keep this on the ZC fast path.

   The two mechanisms compound — the 76 µs/RPC end-to-end gap
   between jumbo_shm (98 K ns/op) and UDS (174 K ns/op) is
   consistent with the combined kernel-side reduction and
   transport-internal copy reduction measured in §2.5, after the
   proto cache-locality tradeoff noted in §2.5.2. Neither mechanism
   is reachable from the UDS path while preserving the `net.Conn`
   abstraction.

5. The data supports treating UDS and SHM as complementary: UDS remains
   the portable same-host socket baseline; SHM targets deployments
   willing to adopt a non-`net.Conn` transport for lower CPU and
   latency. The measured gap is rooted in the OS interface boundary and
   will not close through gRPC-Go code iteration alone.

---

**Notes.**

- `cpu-ns/op` is aggregate CPU across cores; it is not divisible by
  syscall count to recover wall-time per syscall.
- TCP loopback was omitted from this note — UDS is the stronger
  same-host socket baseline and was the optimization target Doug raised.
- Abstract namespace UDS and `SOCK_SEQPACKET` were not measured
  separately: both still use the AF_UNIX socket data path (copy + wake
  + queue) and would not be expected to close the kernel-boundary gap.
- Single-cell ranges across the 3 runs are tight (within ~3 % for
  ns/op); raw runs are in the appendix bundle.

**Appendix.** Raw `go test -bench` output, CPU profile, block profile
(`.pb`) artefacts, and `/proc/<pid>/io` snapshots for all measured
cells are available on request.
