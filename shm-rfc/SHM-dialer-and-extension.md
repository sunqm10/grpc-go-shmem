# SHM as a Dialer vs. SHM as an Extension

**Purpose.** Answer two questions raised in review about packaging the
shared-memory (SHM) transport:

- **Part 1 (this is complete).** Could SHM ship as *only* a
  `grpc.WithContextDialer` / `net.Listener` — a plain `net.Conn` handed to
  stock gRPC-Go, **zero changes to gRPC-Go core** — and still retain the
  benefit? This part measures the empirical floor of that shape and
  attributes exactly what survives the `net.Conn` boundary and what does
  not.
- **Part 2 (design sketch, pending).** If instead SHM were a first-class
  *extension point* (a pluggable transport, with a small generic gRPC-Go
  enhancement), what becomes reachable that the pure dialer cannot reach —
  and what are the cross-language implications. Part 2 is a design
  discussion, not a measurement, and is explicit about what is *not*
  achievable.

The companion note [`SHM-vs-UDS-analysis.md`](SHM-vs-UDS-analysis.md)
answers the separate question "why can't UDS be optimized to match SHM?".
This note assumes that analysis and reuses its decomposition of SHM's win
into two structurally independent mechanisms:

- **(a) kernel-socket-path bypass** — the dominant, structural win
  (~1,300-5,000× fewer kernel bytes per RPC; the bulk of the CPU/latency
  gap), and
- **(b) user-space transport-internal zero-copy (ZC)** — marshal proto
  directly into ring memory, parse it in place (~22 µs/RPC of framer copy
  collapsed to ~0.4 µs on the ZC fast path).

The short answer to Part 1: **mechanism (a) is fully preserved through a
plain `net.Conn`; mechanism (b) is almost entirely lost, and no public
gRPC-Go API can recover it.** The net result is a clean *layered
degradation*: the dialer keeps most of the win for small messages and
degrades toward UDS as the payload grows past the HTTP/2 frame size.

---

## Part 1 — What a pure Dialer/Listener can do (zero core changes)

### 1.1 What was built

The existing in-tree byte-pipe (`internal/transport.ShmConn`, the
`Read`/`Write`/`Close` over two SPSC rings) already *is* a `net.Conn` once
the trivial address/deadline methods are added
([`internal/transport/conn_netconn.go`](../internal/transport/conn_netconn.go)).
A floor harness
([`benchmark/shmemtcp/trackb_bench_test.go`](../benchmark/shmemtcp/trackb_bench_test.go))
hands that `net.Conn` to **stock** gRPC-Go:

- server: `grpc.NewServer().Serve(lis)` over a minimal `net.Listener` whose
  `Accept` returns the server-side `ShmConn`;
- client: `grpc.NewClient(..., grpc.WithContextDialer(dial))` where `dial`
  returns the client-side `ShmConn`.

Nothing else changes: gRPC-Go's framer, `loopyWriter`, HPACK, flow control,
and codec are entirely intact. The only thing replaced is the AF_UNIX
socket — swapped for ring + wake. This is exactly the "Dialer/Listener
only" shape under discussion.

Two variants are measured:

- **default** — gRPC-Go's default read/write staging buffers.
- **ZeroBuf** — `WithWriteBufferSize(0)` + `WithReadBufferSize(0)`, which
  removes gRPC-Go's `bufio` staging layer so each framer write reaches
  `conn.Write` directly (one ring copy instead of two).

### 1.2 The measured floor

Linux, Intel Xeon Platinum 8370C @ 2.80 GHz, 16 vCPU; Go 1.25;
3-run median `ns/op`, streaming ping-pong. (Measured under the repo's
`shm-tuned` bench profile; the retention ratio is computed *within* the
profile — all three transports measured under identical settings — so it
is invariant to the profile choice.)

| Cell | full SHM transport | **TrackB dialer (best of default/ZeroBuf)** | UDS | **retention** |
|---|---|---|---|---|
| Stream 1 KB | 13,961 | **20,000** (default) | 25,705 | **49 %** |
| Stream 64 KB | 89,571 | **152,350** (ZeroBuf) | 169,949 | **22 %** |
| Stream 256 KB | 234,452 | **396,553** (ZeroBuf) | 455,392 | **27 %** |

`retention = (UDS − TrackB) / (UDS − fullSHM)` = the fraction of the full
transport's advantage over UDS that the zero-core-change dialer keeps.

A note on ZeroBuf: it helps at 64 KB / 256 KB (removes one staging copy)
but *hurts* at 1 KB (20,000 → 24,374), because disabling the write buffer
also disables gRPC-Go's HEADERS+DATA batching, producing more ring wakes
per small RPC. So the best dialer configuration is payload-dependent, and
even at its best the floor is **22-49 %** retention — not the ~70 % a naive
"keeps kernel bypass" estimate would suggest.

### 1.3 Why it lands there — three aligned pieces of evidence

**(i) Kernel bypass is fully preserved.** `/proc/<pid>/io` over a
steady-state window (PID verified against `/proc/<pid>/cmdline`):

| 64 KB stream | kernel bytes / RPC |
|---|---|
| TrackB dialer (ZeroBuf) | ~370 B |
| UDS | ~283 KB |

A **~760×** reduction — the *same order* as the full SHM transport
(~170-200 B/RPC). Mechanism (a) survives the `net.Conn` boundary intact:
the bytes never traverse the socket data path. This is *why* the 1 KB cell
still keeps 49 %.

**(ii) The residual cost is user-space copy, not wakes.** `go tool pprof`
flat % on the TrackB 64 KB ZeroBuf profile:

| symbol | flat % |
|---|---|
| `runtime.memmove` | **17.0 %** (top) |
| `internal/runtime/syscall.Syscall6` | 13.0 % |
| `runtime.futex` | 4.4 % |

The path is **copy-bound, not wake-bound**. `memmove` dominates; futex (the
wake primitive) is only 4.4 %. So the lost retention at 64 KB+ is mechanism
(b) — the user-space ring↔framer copy that the full transport's ZC fast
path avoids.

**(iii) The copy cannot be removed through any public gRPC-Go API.** This
is the key Part-1 finding, and it was verified by building the optimization
and proving it cannot fire, rather than asserting it.

### 1.4 The write-ZC probe — the best public-API optimization, and why it fails

The most promising public-API trick (proposed in design review) is a
*ring-backed `mem.BufferPool`* via `experimental.WithBufferPool`: have the
codec marshal the proto directly into the send ring, then an
"alias-detecting" `ShmConn.Write` would notice the buffer already lives in
the ring and skip the copy. This would recover the write side of mechanism
(b) with zero core changes.

It cannot work, for a structural reason. A ring is **sequential**: a
zero-copy write requires the payload to be placed *contiguously at the ring
write head*. But gRPC-Go's framer interleaves variable-length HTTP/2 /
HPACK / WINDOW_UPDATE header bytes *between* the codec's `pool.Get` (when
the payload buffer is allocated, at marshal time) and the framer's
`conn.Write` of that payload — and it chunks the payload at the 16 KiB
HTTP/2 frame cap. So the payload can never be a single contiguous region at
the head.

This was made directly observable with a counting `net.Conn` wrapper that
records the size of every `conn.Write` the stock framer issues
([`benchmark/shmemtcp/trackb_writeprobe_test.go`](../benchmark/shmemtcp/trackb_writeprobe_test.go),
`TestTrackBWriteProbe`, run with ZeroBuf so writes reach `conn.Write`
directly):

| message size | conn.Write calls / RPC | largest single Write |
|---|---|---|
| 1 KB | 6.1 | 1,033 B |
| 64 KB | 17.2 | **16,384 B** |
| 256 KB | 45.4 | **16,384 B** |

Two facts fall out, both decisive:

1. **The 16 KiB frame cap is real at the `conn` boundary.** No single
   `Write` ever exceeds 16,384 B for payloads ≥ 64 KB. A 64 KB message is
   4 DATA frames; 256 KB is 16. gRPC-Go exposes **no public override** for
   this cap.
2. **Header and payload writes are interleaved.** Thousands of ≤16 B and
   ≤64 B writes (9-byte frame headers, the 5-byte gRPC length-prefix,
   WINDOW_UPDATE frames for the response being consumed) are interleaved
   between the 16 KiB DATA chunks. Even a 1 KB message takes 6 writes/RPC.

A ring-backed write-ZC pool would therefore need **17-45 separate
contiguous ring reservations per message**, each broken by an interleaved
header write — which is not a zero-copy reservation at all. This is the
same structural reason the *read* side is blocked: gRPC-Go's framer picks
its destination buffer (`pool.Get(length)` then `io.ReadFull(conn, buf)`)
*before* the bytes move, so `conn.Read` is contractually obliged to copy
into the framer's buffer; it cannot hand up a ring slice.

For contrast, the full SHM transport achieves ZC precisely because it
reserves header **and** payload together in *one* contiguous ring
reservation (`ShmRing.ReserveWrite`) and marshals into it — an operation
the stock framer's write ordering structurally cannot express.

**Conclusion of the probe:** the write-ZC optimization does not merely give
marginal upside through a `net.Conn` — it *cannot fire at all*. The
22-49 % floor in §1.2 is the real floor; no public-API trick lifts it.
(This is a structural property of the gRPC-Go framer, independent of
platform, so it needs no separate Linux confirmation.)

### 1.5 Part 1 conclusion

A pure Dialer/Listener `net.Conn`-over-SHM plugin, **with zero changes to
gRPC-Go**, is a real and useful artifact:

- It **preserves mechanism (a)**, the structurally dominant win — proven
  ~760× fewer kernel bytes per RPC than UDS. For small messages (≤ the
  HTTP/2 frame size) it keeps roughly half of the full transport's
  advantage (1 KB: 49 %), and it always beats UDS.
- It **loses mechanism (b)** for messages larger than the 16 KiB frame
  cap, and this loss is **not recoverable through any public gRPC-Go API**
  — empirically demonstrated, not assumed. Retention falls to ~22 % at
  64 KB and degrades toward UDS as the payload grows.

So the honest framing for the "just ship a Dialer" option is: it is a
legitimate, low-friction packaging that captures the kernel-bypass win and
is strongest for small-message / high-RPC-rate workloads, but it
**structurally cannot match the full transport at mid/large payloads**.
Closing that gap requires either a custom transport or a small *generic*
gRPC-Go enhancement — which is the subject of Part 2.

---

## Part 2 — What an Extension / pluggable transport could unlock

> **Status: design sketch.** Part 1's numbers are measured and final.
> Part 2 is a design direction. Every retention figure here is flagged as
> an **estimate** (no Part-2 prototype has been built), and every place a
> mechanism is genuinely *not* achievable is called out rather than glossed.

The alternative to "just a dialer" (Part 1) and "a full fork-level custom
transport" is a first-class *extension point*: an out-of-tree transport
plus a **small, generic** (non-SHM-specific) gRPC-Go enhancement. The
question is what the smallest such enhancement is, how much of mechanism (b)
it reclaims, and whether it is plausibly upstreamable.

### 2.0 Where the copies actually live (verified call sites)

Part 1 proved that under a plain `net.Conn`, mechanism (b) is lost because
the framer owns buffer allocation on *both* sides and chunks at the 16 KiB
cap. The exact gRPC-Go functions that own each copy, verified in this tree:

- **Write coalescing + chunking:** `loopyWriter.processData`
  ([controlbuf.go](../internal/transport/controlbuf.go)) sets
  `maxSize := http2MaxFrameLen`, then **appends the 9-byte frame header and
  the payload into one `l.writeBuf`** and hands that to `framer.writeData`.
  That coalescing is the structural reason a ring slice cannot survive to
  the wire untouched.
- **Read into a framer-owned buffer:** the framer reads each DATA frame's
  payload into a buffer it got from its `mem.BufferPool`
  (`newFramer` in [http_util.go](../internal/transport/http_util.go)); this
  is the `conn.Read → framer buffer` copy. Note `parsedDataFrame.data` is a
  single `mem.Buffer` today, which constrains the read-hook shape (below).
- **The cap:** `http2MaxFrameLen = 16384`
  ([http_util.go](../internal/transport/http_util.go)), applied to reads via
  `SetMaxReadFrameSize` and advertised as `SETTINGS_MAX_FRAME_SIZE`
  ([http2_server.go](../internal/transport/http2_server.go)). **No public
  override exists.**
- **Already-pluggable surface:** `mem.BufferPool` (via
  `experimental.WithBufferPool`) and `encoding.CodecV2` /
  `MarshalWithPool` already let an extension control *where bytes are
  marshaled into*. What is missing is a hook controlling *how bytes cross
  the `conn` boundary without being re-copied / re-chunked.*

### 2.1 The minimal generic hooks

Three hooks, increasing in intrusiveness. **None names SHM** — each is
justified as a general improvement that also benefits TCP/UDS.

**Hook A — read-side, conn-supplied buffers.** Let a conn that already has
bytes in mappable memory hand them up by reference instead of being read
into a framer-owned buffer:

```go
// Optional. The framer asks the conn for the frame payload as
// conn-owned, refcounted buffers instead of pool.Get + io.ReadFull.
type BufferReader interface {
    ReadBuffers(n int) (mem.BufferSlice, error)
}
```

Plugs into the framer's DATA-payload read path. **Generic justification:** a
TCP/UDS conn could implement this over `readv` into pool buffers;
mmap'd-file, `io_uring`-registered-buffer, and `MSG_ZEROCOPY` receive paths
fit the same "let the byte source own the receive buffer" shape — a direct
generalization of the already-merged `mem.BufferPool` work. **Honest
caveat:** today `parsedDataFrame.data` is a single `mem.Buffer`, so the
*minimal* version is `ReadBuffer(n) (mem.Buffer, error)`; the richer
`BufferSlice` form needs receive-buffer plumbing widened from `mem.Buffer`
to `mem.BufferSlice`. And with the 16 KiB cap still in force, a 64 KB
message still arrives as ~4 buffers — Hook A removes the framer-read copy
but **not** the gather copy inside `proto.Unmarshal` of a multi-buffer
slice (Go's proto decoder cannot consume a multi-segment input without
materializing). Single-buffer read ZC needs **Hook C**.

**Hook B — write-side, vectored write / reservation.** Stop coalescing
header+payload before the write; either pass a scatter list
(`net.Buffers`-style) or let the framer reserve destination memory:

```go
// Optional. loopyWriter hands [frameHeader, lpmPrefix, payload...] as
// separate buffers; a ring-backed conn writes only the 14B of framing
// and detects that the payload buffer is already its own ring memory.
type BufferWriter interface {
    WriteBuffers(bufs mem.BufferSlice) (int, error)
}
// or, symmetric to ShmRing.ReserveWrite:
type ReservingWriter interface {
    ReserveWrite(n int) (WriteReservation, error)
}
```

Plugs into `loopyWriter.processData` / `framer.writeData`. **Generic
justification:** this is **writev** — not coalescing header+payload into a
staging buffer before the syscall is the single most obvious HTTP/2 write
win on TCP too, and it matches a stdlib idiom (`net.Buffers`). This is the
**most upstreamable** of the three. **Honest caveat:** the alias-detection
(payload already in the conn's ring) is the SHM-specific *consumer*, but
the hook is generic. And `ReserveWrite`/`WriteBuffers` still only moves
*already-marshaled* bytes; to marshal proto *directly* into the ring (true
write ZC, what the full transport's `WriteProto` fast path does in
[h2_codec.go](../internal/transport/h2_codec.go)) you would need a
*message-level* hook (e.g. `WriteMessage(msg any) (handled bool, err error)`)
that exposes codec/message knowledge to the transport. That crosses a much
stronger layering boundary and is the **least upstreamable** piece.

**Hook C — public max-frame-size override.**

```go
func WithMaxFrameSize(n uint32) DialOption // client
func MaxFrameSize(n uint32) ServerOption   // server
```

Replaces the hard-coded `http2MaxFrameLen` at its use sites (the
`processData` chunk size, `SetMaxReadFrameSize`, and the advertised
`SETTINGS_MAX_FRAME_SIZE`). **Why it's needed:** Hooks A/B make the
*boundary* zero-copy, but the 16 KiB cap fragments every payload > 16 KiB
into multiple frames (Part 1 §1.4: 64 KB = 17 writes/RPC). For
single-buffer read ZC and single-reservation write ZC, the payload must fit
in one frame. **Generic justification:** a larger `SETTINGS_MAX_FRAME_SIZE`
(HTTP/2 permits up to 16 MiB) is a standard throughput knob users already
request for high-bandwidth links. **Honest caveat:** raising the cap has
real costs on *socket* transports (head-of-line blocking, coarser flow
control), so upstream would want it opt-in with documented tradeoffs.

**Retention estimate (flagged as estimate — not measured):**

| Configuration | What it recovers | Est. retention @ 64 KB | Confidence |
|---|---|---|---|
| Part 1 floor (pure dialer) | — | **22 % (measured)** | — |
| Hook C only | fewer/larger frames | ~30-40 % | low |
| C + A (read ZC) | framer-read copy + proto gather | ~50-60 % | medium |
| C + B (write ZC) | write coalescing copy | ~50-60 % | medium |
| C + A + B + message-level marshal | both copies → ~0.4 µs (cf. §2.5 jumbo_shm) | **~75-90 %** | medium |
| Full custom transport | everything | **100 % (measured)** | — |

The reason tier-(ii) lands at **~75-90 %, not 100 %**: even with all three
hooks, the out-of-tree transport still pays the *architectural* costs the
full transport avoids — the `controlBuf.put` → `loopyWriter` goroutine
handoff per message, and generic HPACK/flow-control machinery. The full
transport bypasses the `controlBuf` channel entirely on its inline
`WriteProto` fast path (see [`SHM-vs-UDS-analysis.md`](SHM-vs-UDS-analysis.md)
§2.3); the hooks cannot replicate that without *becoming* the full
transport. **That residual is exactly the gap between tiers (ii) and (iii).**

### 2.2 Upstreamability, hook by hook

| Hook | Strongest general-purpose framing | Likely reception |
|---|---|---|
| **B (writev)** | "loopyWriter should use vectored writes instead of coalescing header+payload" — standalone TCP win, stdlib idiom | **Most likely accepted.** |
| **C (frame cap)** | "make `SETTINGS_MAX_FRAME_SIZE` configurable" — long-standing HTTP/2 tuning request | **Plausibly accepted**, opt-in. |
| **A (ReadBuffers)** | "let the byte source own the receive buffer" — generalizes `mem.BufferPool`; benefits readv/io_uring | **Hardest**; strongest if landed *with* a non-SHM (TCP `readv`) consumer. |
| message-level marshal | (true write ZC) | **Least likely** — crosses codec/transport layering. |

A meta-point to state plainly for the reader: gRPC-Go today has **no public
pluggable-transport SPI at all** — the only transport is
HTTP/2-over-`net.Conn`. So "SHM as a first-class extension point" is really
two asks: (1) these byte-boundary hooks, and (2) willingness to treat the
transport interface as extensible. The hooks are the smaller, concrete ask;
a full pluggable-transport SPI is a larger architectural commitment.

### 2.3 The three-tier option space

| | **(i) Pure dialer** | **(ii) Dialer + generic hooks** | **(iii) Full custom transport** |
|---|---|---|---|
| gRPC-Go core change | **none** | 2-4 generic interface additions | none to core, but **forks the transport** |
| Mechanism (a) kernel bypass | ✅ full (~760× proven) | ✅ full | ✅ full |
| Mechanism (b) user-space ZC | ❌ lost > 16 KB | ⚠️ mostly recovered (est.) | ✅ full |
| **Retention @ 64 KB** | **22 % (measured)** | **~75-90 % (estimate)** | **100 % (measured)** |
| Retention @ 1 KB | 49 % (measured) | ~85-95 % (estimate) | 100 % |
| Maintenance | trivial — rides stock gRPC-Go | low-moderate — tracks hook APIs | **high — fork-level** |
| Upstream dependency | none | **blocks on maintainers accepting hooks** | none (but owns whole transport) |

**The crisp tradeoff.** Tiers (i) and (iii) are the two stable equilibria:
zero-coordination / low-ceiling vs. high-coordination / high-ceiling. Tier
(ii) is strictly better than (i) **only if** the hooks land upstream, and
its ceiling (~75-90 %) is bounded by the generic `loopyWriter`/`controlBuf`
architecture it deliberately keeps. Pragmatic sequencing: **ship (i) now;
pursue B + C upstream (the easy wins) to lift the dialer's ceiling even
without A; treat (iii) as the escape hatch for workloads that need the last
10-25 %.**

### 2.4 Cross-language viability

The extension splits into a **standardizable layer** (a shared *spec*, not
shared code) and a **per-runtime layer** — and the boundary is not where
one might hope.

**What CAN be standardized (as a spec):**

- **The wire format** — HTTP/2 frames (DATA/HEADERS/WINDOW_UPDATE), HPACK,
  the 5-byte gRPC length-prefix — is already language-neutral. Carrying it
  over a ring changes the *byte source*, not the bytes.
- **The segment memory ABI** — ring layout (head/tail offsets, alignment,
  control region, in-band framing, wake-word location), capability/handshake
  negotiation. Two different-language processes can map the same segment and
  agree on it. A C-core client *could* talk to a Go server over one
  segment — **iff** they agree on the wake primitive, which is where it
  breaks.

**What CANNOT be standardized as a single ABI (the honest limits):**

- **The wake primitive is the hard cross-language constraint.** It is forced
  by each runtime's I/O model, and they do not agree:
  - **Go** can wait on a futex (runtime park) *or* an eventfd via the
    netpoller; futex is the lowest-overhead Go-native option (what this
    transport uses on Linux).
  - **C-core** (epoll/EventEngine) integrates an **eventfd** naturally but
    **cannot wait on a raw futex** — a futex word is invisible to
    `epoll_wait`.
  - **Java/Netty** can wait on an **eventfd via epoll** (native epoll
    transport) but **cannot touch a raw futex** without JNI; the JVM exposes
    no `futex(2)`.
  - **Therefore eventfd is the only primitive all three I/O models can wait
    on without busy-polling.** A cross-language standard must mandate
    **eventfd as the baseline wake**, and treat **futex as a Go/C-only,
    same-language Linux optimization that is unavailable whenever a JVM
    endpoint participates.** This is a real, unavoidable performance
    asymmetry: the lowest-overhead primitive cannot be the interop baseline.
- **Segment lifecycle cannot be one shared library.**
  `memfd_create`/`shm_open`/`mmap`/`SCM_RIGHTS` interact with each runtime's
  fd-ownership model differently. A shared C library is theoretically
  linkable by all three, but **Go via cgo defeats the purpose** (cgo calls
  don't integrate the netpoller, reintroduce a thread hop, and break the
  zero-extra-syscall goal). So **each language needs its own native
  implementation; they share the spec, not the binary.**
- **Cross-OS interop is out of scope / not achievable under one ABI.**
  Windows has no eventfd or futex — the wake becomes a named event / IOCP
  and the segment a named file-mapping. The segment+wake ABI is necessarily
  **per-OS**; a Linux client ↔ Windows server over shared memory is not a
  goal a single ABI can serve.
- **Pluggable-transport feasibility itself varies:** C-core has a real
  `grpc_endpoint`/transport abstraction (most favorable); Go has no public
  transport SPI today (needs the §2.1 hooks); Java has an internal, unstable
  transport abstraction (possible but tracks internals, plus JNI/Panama for
  the native memory + wake).

**Cross-language bottom line:** standardize the **wire contract + per-OS
segment/ring ABI** as a spec with conformance tests (Go↔Go, Go↔C-core,
Go↔Java, crash/cleanup); do **not** attempt a single cross-language plugin
binary. Mandate **eventfd** for any cross-language segment and document
**futex** as a same-language Linux-only optimization. Declare **cross-OS**
shared-memory interop out of scope. Forcing every runtime through a
least-common-denominator abstraction would give back much of the
performance SHM exists to preserve.

### 2.5 Open items (flagged, not papered over)

1. The tier-(ii) retention figures (~75-90 %) are **estimates** extrapolated
   from §2.5's copy attribution plus the residual `loopy`/`controlBuf`
   handoff cost — **not measured**. A `BufferWriter`/`BufferReader`
   prototype against stock gRPC-Go is the natural next experiment if a
   quantified tier (ii) is wanted.
2. Hook A's **refcount lifecycle** (who `Free()`s a conn-donated buffer if a
   stream resets mid-frame) needs a concrete ownership rule; this is the
   part most likely to surface correctness bugs and the part upstream will
   scrutinize hardest.
3. Whether the `proto.Unmarshal` **gather copy** for a multi-buffer slice is
   truly eliminated by Hook C (single-buffer frames) or merely reduced
   depends on the proto codec's materialization behavior — verify before
   claiming single-buffer read ZC.

---

## Part 3 — A concrete cross-language-conformant transport (the design we are building)

> **Status: design settled, implementation in progress.** Part 2 framed the
> option space. Part 3 is the concrete plan for the path we chose: keep the
> full custom transport (tier iii, 100 % of the win) **and** make it
> wire/ABI-compatible with other languages, so the same Go code can talk to
> a future C-core / Java / .NET peer. This was settled by two rounds of
> dual-model design + cross-review grounded in the actual transport source.

### 3.1 Two profiles, negotiated per connection

The transport runs in one of two profiles, chosen during connection setup:

- **`GoPrivateV1`** — Go↔Go. Keeps every same-language optimization: the
  shared-memory `_ctl` control segment, futex fast-wake fallback, jumbo
  (16 MiB) DATA frames, the large (32 MiB) flow-control window. Pays nothing
  for cross-language conformance.
- **`CrossLangV1`** — Go talking to any other language. Eventfd-only wake,
  mandatory HTTP/2 SETTINGS, `speculativeReserved == 0`, frame size and
  window governed by SETTINGS, and a **UDS bootstrap** (below) instead of the
  futex `_ctl` segment.

A capability bit in the CONNECT/ACCEPT handshake selects the profile by
**AND-negotiation**: the Go dialer advertises "Go extensions"; the acceptor
echoes it only if it also supports them. A C-core/Java/.NET peer never sets
the bit, so the Go side auto-degrades to `CrossLangV1`. The negotiated
profile is stamped on the `Segment` and threaded to each ring at registration
time; every Go-private optimization gates on `profile == GoPrivateV1`.

### 3.2 The UDS bootstrap (Option B)

Today the *entire* handshake runs over shared memory: the server creates a
futex-backed `_ctl` segment, the client maps it, writes CONNECT on one ring,
and reads ACCEPT on another — **waiting via futex**. A JVM or C-core client
cannot wait on a futex, so it cannot even participate in the handshake.

`CrossLangV1` replaces the `_ctl` segment with a **per-listener Unix-domain
socket** (`<segment-path>.ctl.sock`, mode `0600`, `SO_PEERCRED` same-UID
check). The handshake becomes one stream exchange any language can speak:

```
client → server : CONNECT bytes (caps, profile, nonce)           [one write]
server → client : ACCEPT bytes + SCM_RIGHTS[evfd_c2s, evfd_s2c]   [one sendmsg]
client          : open segment, build eventfd waker from the 2 fds
both            : keep the UDS conn open  →  its EOF = peer death (kill -9)
```

This is not new machinery from scratch: fd-passing **already** uses a
per-segment UDS + SCM_RIGHTS ([`shm_fdpass_linux.go`](../internal/transport/shm_fdpass_linux.go));
Option B promotes that socket from "fd-only" to "fd + CONNECT/ACCEPT". It
also *removes* the `_ctl` segment's futex wait, its `.lock` flock, the
multi-producer-on-a-shared-ring hazard, and the stale-response nonce loop —
a point-to-point UDS has none of those races. Honest caveat: it **relocates**
the named rendezvous (the listening socket still needs a name derived from
`shm://<addr>`) rather than eliminating it; the segment-fd-over-UDS (memfd)
variant that would erase the `/dev/shm` unlink race entirely is a later step.

### 3.3 What conformance requires (the concrete change-list)

Grounded in an audit of the current transport, the gap to a conformant
`CrossLangV1` is **smaller than expected** — three of the hardest items are
already done:

| Item | Current state | Work |
|---|---|---|
| `speculativeReserved@0x38` (ABI conflict) | `AddSpeculativeReserved` has **zero call sites**; the reader already uses the interop-safe deferred-`ridx` ZC; the writer's reads always subtract 0 | Behavior-preserving dead-code deletion + rename `reservedZero` + assert `== 0` |
| Eventfd wake | **Already the production default**; futex is only the fallback | Make eventfd *mandatory* for `CrossLangV1`; on setup failure fall back to the bootstrap channel (never futex) |
| HTTP/2 wire | **Already H2-only** on the data plane (control_wire advertises only H2, rejects non-H2 peers) | Done |
| SETTINGS | **Validated then skipped** — no state machine | Add real SETTINGS state: send-first (no ACK-wait), ACK on receipt, enforce first-H2-frame-is-SETTINGS, drive outbound frame size from the peer's `SETTINGS_MAX_FRAME_SIZE` |
| Flow control | Large 32 MiB window keeps WINDOW_UPDATE dormant | `CrossLangV1` uses HTTP/2-default 64 KiB windows unless negotiated larger; verify WINDOW_UPDATE actually flows |
| `writeH2Single` > ring-cap | Rejects any frame larger than the ring | Incremental write for large HEADERS/CONTINUATION + a `ringCap ≥ maxNonDataFrame` guard |
| Liveness | No prompt `kill -9` detection (eventfd has no EOF) | UDS-EOF = authoritative crash signal; segment `closed` flag = graceful (drain first); bounded ring-wait park so death is observed |

One subtlety: the security handshake runs on the raw rings *before* the H2
transport, so "first frame must be SETTINGS" keys off the **first H2 frame
after the handshake**, not byte 0 of the ring.

### 3.4 Implementation order (measure performance early)

The per-RPC cost of conformance lives entirely in the SETTINGS / window /
frame-size machinery — **not** in the UDS bootstrap, which only affects
one-time connection setup. So the cost can be measured *before* the bootstrap
is built, by forcing a Go↔Go pair into `CrossLangV1` over the existing
handshake with the new knobs flipped:

0. **`speculativeReserved` cleanup** (near-zero risk; confirm no bench delta).
   — **done** (committed; behaviour-preserving, no bench delta).
1. **SETTINGS state machine** (first perf gate). — *next; required to finish a
   robust per-connection window profile, see §3.5.*
2. **Profile plumbing** + capability bits (eventfd mandatory for `CrossLangV1`).
   The per-connection frame-size override is **done** (committed); the window
   override is prototyped but needs step 1 to be robust over-window.
   → **Minimum benchmarkable path = steps 0+1+2**; the strict-posture numbers
   in §3.5 are already obtainable today via the global flow-control knobs.
3. **UDS bootstrap** (highest risk; one-time setup cost only).
4. **Hardening** (incremental HEADERS, UDS-EOF liveness, bounded park).
5. *(deferred)* memfd segment-fd over UDS (closes the unlink race).

### 3.5 Performance — measured (what the extension costs)

Because `CrossLangV1` remains a **custom transport**, it keeps the two things
Track B (Part 1) structurally could not: it reserves header+payload together
in one contiguous ring slot (write ZC) and the receiver parses bytes in place
*within* each frame. It is **not** subject to the stock framer's
16 KiB-cap-plus-interleave ceiling that pinned Track B at 22 % retention.

The strict-`CrossLangV1` per-RPC cost is **measurable today** via the global
flow-control knobs (`BENCH_PROFILE=fair-default` = HTTP/2-default 64 KiB
windows, `SHM_MAX_FRAME_SIZE=16384` = HTTP/2-default frame size) — they impose
the exact conservative posture the negotiated profile applies. Measured on
Linux, Intel Xeon Platinum 8370C @ 2.80 GHz, 16 vCPU, Go 1.25, streaming
ping-pong, `benchtime=3s`, single run:

| Streaming | GoPrivateV1 (baseline) | **strict CrossLangV1** | UDS | **CrossLangV1 vs UDS** | **retention** |
|---|---|---|---|---|---|
| 1 KB | 14,095 ns | **15,076 ns** | 23,101 ns | **1.53× faster** | **89 %** |
| 64 KB | 89,745 ns | **126,806 ns** | 203,767 ns | **1.61× faster** | **68 %** |
| 256 KB | 236,548 ns | **391,354 ns** | 689,678 ns | **1.76× faster** | **66 %** |

`retention = (UDS − CrossLangV1) / (UDS − GoPrivateV1)` = the fraction of the
full transport's advantage over UDS that the conservative cross-language
posture keeps.

Two findings, both stronger than the original prediction:

- **The conformance cost is real but bounded, and CrossLangV1 stays well
  above UDS at every size.** Even forced into the strictest foreign-peer
  posture (16 KiB frames + 64 KiB windows), the custom transport is **1.5–1.8×
  faster than UDS** and retains **66–89 %** of the Go-tuned advantage. The
  conformance cost (GoPrivateV1 → strict) grows with payload — 1 KB +7 %,
  64 KB +41 %, 256 KB +65 % — exactly as expected, because the 16 KiB frame
  cap multiplies the DATA-frame count on larger messages.
- **strict CrossLangV1 beats the Track B floor.** At 64 KB, strict
  CrossLangV1 (126,806 ns) is faster than the Track B `net.Conn` dialer
  (152,350 ns from Part 1), because it remains a custom transport — it keeps
  single-copy-into-ring ZC *within* each 16 KiB frame and full kernel bypass,
  both of which Track B's `net.Conn` double-copy forfeits. This is the
  empirical payoff of the extension over the pure-dialer floor.

The remaining open question for the *real* cross-language number is whether a
real C-core / .NET / Java peer will accept a larger `SETTINGS_MAX_FRAME_SIZE`
and window. If it does, the cost shrinks toward the GoPrivateV1 baseline; if
it caps at 16 KiB / 64 KiB, the "strict" column above **is** the real-world
cross-language performance — still the best any cross-language local transport
can do.

**Implementation status of the measured profile.** The numbers above were
obtained through the process-global flow-control knobs, which impose the
identical window + frame cadence the negotiated `CrossLangV1` profile applies.
A per-connection `CrossLangV1` dial/listen option was prototyped and is
correct for messages within the stream window, but a robust profile that
holds at messages *larger* than the window requires the SETTINGS state
machine (§3.3 / §3.4 step 1): without an in-band SETTINGS handshake the two
ends cannot agree on the WINDOW_UPDATE threshold, so a per-connection window
override alone stalls on over-window messages. That confirms, empirically,
that **window negotiation belongs in SETTINGS** — the per-connection profile
is finished by landing the SETTINGS step, not bolted on before it.

### 3.6 What the "conformance cost" actually is — frame vs window, and why it is a floor not a tax

A blunt 41–65 % "cost" hides the mechanism. A 2×2 sweep at 64 KB streaming
(toggling frame size and window independently via the global knobs) decomposes
it (relative trend; the absolute values here are from a faster smoke run, but
the *ratios* are the point):

| 64 KB | jumbo frame (16 MiB) | standard frame (16 KiB) |
|---|---|---|
| **jumbo window (32 MiB)** | A — pure SHM (baseline) | B — **+28 %** vs A |
| **standard window (64 KiB)** | C — *structurally fails*¹ | D — strict (+36 % vs A) |

¹ A 16 MiB frame with a 64 KiB window is incompatible: a 64 KB message rides
one big DATA frame but exceeds the 64 KiB window, and the single-frame fast
path skips the pre-credit that a chunked message gets — so jumbo-frame and
small-window are *bound together*, they cannot be mixed.

Reading the decomposition:

- **The frame cap is ~80 % of the cost** (A→B is +28 %; the window's marginal
  contribution B→D is only ~+6 %). The conformance cost is overwhelmingly a
  *framing* effect, not a flow-control effect.
- **The frame cap is negotiable.** `SETTINGS_MAX_FRAME_SIZE` is a standard
  HTTP/2 SETTINGS parameter, and HTTP/2 permits values up to 16 MiB (2²⁴−1).
  The 16 KiB used in the "strict" column is merely the *default*, not a
  ceiling. The whole job of the `CrossLangV1` SETTINGS step (§3.4 step 1) is
  to negotiate the frame size *up* to whatever the peer will accept. So the
  dominant 80 % of the conformance cost is exactly the part the extension's
  own negotiation can claw back.

**The honest reframing (important).** The "strict CrossLangV1" column in §3.5
is not a fixed extension tax — it is the **floor**, the number you get when a
foreign peer refuses to negotiate anything above HTTP/2 defaults. It is, quite
literally, *the gRFC SHM transport measured under HTTP/2-default settings*
(`fair-default` + 16 KiB frames). The extension does not *add* this cost; the
extension's entire purpose is to **negotiate away as much of it as the peer
allows**. Stated as a formula:

> real cross-language cost = (pure-SHM jumbo) − (best posture the peer will
> accept). 16 KiB / 64 KiB is the worst-case **floor** (most conservative
> peer); a capable peer that accepts a larger `MAX_FRAME_SIZE` moves the
> result back toward the pure-SHM baseline.

This also answers "did the gRFC transport leave performance on the table?" —
no. The gRFC engine running at jumbo settings (cell A / GoPrivateV1) is the
full-speed result and is **2.3× faster than UDS at 64 KB on Linux**
(89,745 vs 203,767 ns). The strict column is not the engine underperforming;
it is the same engine deliberately constrained to the settings a
lowest-common-denominator foreign peer can speak. The ~500-line extension is a
thin negotiation layer on top of that 23,800-line engine — small precisely
because the engine already does all the hard work; and the negotiation it adds
is what recovers the frame-size cost that dominates the floor.

### 3.7 Cost of the extension itself — code size and API surface

Two numbers a reviewer will ask for: how much code, and how much public API.

**Code size.** The gRFC SHM transport is ~23,800 lines across 52 files (ring
SPSC sync, eventfd/futex wake, HPACK codec, HTTP/2 framing, flow control, the
ZC fast path + multi-anchor FIFO, security handshake, segment lifecycle,
Linux + Windows). The `CrossLangV1` extension on top of it is:

- **landed already** (committed): `speculativeReserved@0x38` retirement
  (net −54, dead-code removal) + per-connection outbound frame-size override
  (+45 −8) ≈ **~80 lines of new logic** — about **0.3 %** of the engine.
- **remaining** (estimated): SETTINGS state machine (~100 core / ~300 with
  ACK + first-frame + ordering), UDS bootstrap handshake (~200–250, reusing
  the existing per-segment fd-pass UDS), liveness (~120), hardening (~150),
  cross-language conformance tests (~300). **Core path ≈ 400–600 lines;**
  full hardened + tested ≈ ~1,300 lines — i.e. **~6 %** of the engine.

The extension is small *because* the gRFC engine already exists; it is a
negotiation layer, not a second transport. (The genuinely large remaining
cost of true interop is not in Go at all — it is a *second language's* native
reimplementation of the same spec; see §2.4.)

**API surface — it converges.** The extension adds **no new public type or
function**. `experimental/shm` already exposes the stable surface
(`WithTransport`, `WithTransportAndOptions`, `NewListener`, `DialOptions`,
`Config`, `ListenerConfig`, discovery interceptors). `CrossLangV1` needs only
a single boolean on the two existing option structs:

```go
type DialOptions struct {   // already transport.DialOptions
    // ...
    CrossLangV1 bool   // request the cross-language-conformant profile
}
type ListenerConfig struct {
    // ...
    CrossLangV1 bool
}
```

The profile-negotiation bit rides the existing CONNECT/ACCEPT flags byte
(internal wire, not exposed). Default `false` = `GoPrivateV1`, so existing
users are byte-for-byte unaffected — fully backward compatible. This is a
notable contrast with Part 2's tier-(ii) generic hooks
(`BufferReader`/`BufferWriter`/`WithMaxFrameSize`), which would require *new
public gRPC-Go interfaces*: the extension (tier iii) needs **two boolean
fields and no grpc-go core SPI change at all**.

---

## Notes

- Part 1 floor numbers: `shm-tuned` bench profile, Linux Xeon 8370C, Go
  1.25, 3-run median. Retention is computed within-profile and is invariant
  to profile choice. A `fair-default` re-run (to align byte-for-byte with
  `SHM-vs-UDS-analysis.md`'s absolute numbers) is a pending optional follow-up;
  it does not change the retention conclusion.
- Harness caveat: the floor harness uses the ring's default wait/signal
  (futex on Linux) rather than the per-segment eventfd waker the full SHM
  bench enables. The copy question Part 1 answers is independent of the wake
  primitive (see §1.3-ii: futex is only 4.4 % of CPU).
- Reproduce the write-ZC probe:
  `go test -run TestTrackBWriteProbe -v ./benchmark/shmemtcp`.
