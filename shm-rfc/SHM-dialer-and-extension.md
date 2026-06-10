# SHM as a Dialer vs. SHM as an Extension

**Purpose.** Answer two questions raised in review about packaging the
shared-memory (SHM) transport:

- **Part 1.** Could SHM ship as *only* a
  `grpc.WithContextDialer` / `net.Listener` — a plain `net.Conn` handed to
  stock gRPC-Go, **zero changes to gRPC-Go core** — and still retain the
  benefit? This part measures the empirical floor of that shape and
  attributes exactly what survives the `net.Conn` boundary and what does
  not.
- **Part 2.** The approach we are taking instead: keep the full custom
  transport and add a per-connection cross-language-conformant *profile*.
  This part covers the approach, the cross-language feasibility (what can be
  standardized and what cannot), and a comparison against the pure dialer
  and the full same-language transport.
- **Part 3.** How the Go transport is adapted for that profile, the measured
  cost, and an analysis of where the cost comes from and how much of it is
  recoverable by negotiation.

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

## Part 2 — SHM as an extension: approach, feasibility, and trade-offs

Part 1 shows the pure-dialer packaging has a hard ceiling at the HTTP/2 frame
size. The alternative we pursue keeps the full custom transport — the gRFC
SHM engine already in the tree — and makes it **wire- and ABI-compatible with
other gRPC implementations**, so one engine can serve a Go peer at full speed
and a C-core / Java / .NET peer in a standards-conformant mode.

### 2.1 The approach: one engine, two negotiated profiles

The transport selects one of two profiles during connection setup, via a
capability bit in the existing CONNECT/ACCEPT handshake. Negotiation is by
AND: a peer that does not advertise the Go-private capability forces the
conformant profile, so a foreign peer is handled automatically.

- **`GoPrivateV1`** (Go ↔ Go) keeps every same-language optimization: futex
  wake, jumbo (up to 16 MiB) HTTP/2 DATA frames, and a large (32 MiB)
  flow-control window. This is the current transport, unchanged.
- **`CrossLangV1`** (Go ↔ any other language) constrains the engine to what a
  foreign runtime can interoperate with:
  - **eventfd-only wake.** eventfd is the one wake primitive every target
    runtime's I/O loop can wait on (§2.2); the Go-only futex fast path is
    disabled.
  - **SETTINGS-negotiated framing and flow control.** `MAX_FRAME_SIZE` and
    the window are negotiated in-band via HTTP/2 SETTINGS rather than using
    the Go-tuned defaults, so both ends agree on the same values.
  - **A Unix-domain-socket bootstrap handshake** in place of the
    shared-memory control segment. The current handshake exchanges
    CONNECT/ACCEPT over a futex-backed control segment, which a non-Go client
    cannot wait on. `CrossLangV1` carries the same CONNECT/ACCEPT bytes over a
    per-listener UDS and passes the segment + two eventfds via `SCM_RIGHTS` in
    one `sendmsg`. (Fd-passing already uses a per-segment UDS in the current
    transport; this generalizes it.) The UDS connection stays open, and its
    EOF is the peer-death signal.

The data plane in both profiles is **stock HTTP/2 frames + the gRPC
length-prefixed message** — byte-identical to TCP/UDS on the wire; only the
byte transport (ring + eventfd instead of a socket) differs.

### 2.2 Cross-language feasibility

The design separates into a **standardizable layer** (a shared specification,
not shared code) and a **per-runtime layer**.

**Standardizable as a spec:**

- *Wire format* — HTTP/2 frames (DATA / HEADERS / WINDOW_UPDATE), HPACK, and
  the gRPC length-prefix — is already language-neutral. Carrying it over a
  ring changes the byte source, not the bytes.
- *Segment memory ABI* — ring header layout (head/tail indices, capacity,
  wake-word) plus the handshake/capability negotiation. Two processes in
  different languages can map the same segment and agree on it.

**Not standardizable as one binary (the hard constraints):**

- **Wake primitive — eventfd mandatory, futex Go/C-only.** Each runtime's I/O
  model forces the choice: Go can wait on a futex or an eventfd; C-core's
  epoll and a JVM's epoll transport can wait on an **eventfd** but **not on a
  raw futex** (a futex word is invisible to `epoll_wait`, and the JVM has no
  `futex(2)` without JNI). eventfd is therefore the only primitive all three
  can wait on without busy-polling and must be the cross-language baseline.
  The lower-overhead futex stays available only when both ends are Go/C on
  Linux — an unavoidable asymmetry, not a defect.
- **Segment lifecycle is per-language native code.** `memfd_create` /
  `shm_open` / `mmap` / `SCM_RIGHTS` interact with each runtime's fd-ownership
  and I/O integration differently. A shared C library is linkable in
  principle, but Go via cgo would defeat the purpose (cgo calls bypass the
  netpoller and reintroduce a thread hop). Each language implements the
  segment layer natively and shares only the spec.
- **Cross-OS is out of scope.** Windows has no eventfd/futex/SCM_RIGHTS; its
  wake is a named event/IOCP and its segment a named file-mapping. The
  segment+wake ABI is necessarily per-OS, so a Linux ↔ Windows shared-memory
  connection is not a goal a single ABI can serve.

So interop with a second language is a *native reimplementation of the spec in
that language* — the wire and the per-OS segment/ring ABI are shared, the
implementation is not. This is the genuinely large part of "true cross-language
SHM": not the Go-side change (§3), but the second implementation.

### 2.3 Comparison: dialer vs extension vs full transport

| | Pure dialer (Part 1) | **Extension (`CrossLangV1`)** | Full transport (`GoPrivateV1`) |
|---|---|---|---|
| gRPC-Go core change | none | none (two option fields) | none |
| Packaging | stock gRPC-Go + `net.Conn` | custom transport + conformance profile | custom transport |
| Kernel-path bypass | ✅ | ✅ | ✅ |
| In-place user-space ZC | ❌ lost > 16 KiB | ✅ within each frame | ✅ (jumbo frame) |
| Cross-language interop | n/a — Go only | ✅ eventfd + SETTINGS + UDS | ❌ — Go-only fast paths |
| 64 KB streaming vs UDS¹ | ~1.1× | **1.61×** | 2.27× |
| Peer must be | Go | any conformant gRPC impl | Go |

¹ Dialer ratio is from the Part 1 run (UDS baseline 169,949 ns); the extension
and full-transport ratios are from the Part 3 run (UDS baseline 203,767 ns).
The two runs are not directly comparable — read each as a within-run ratio.

The pure dialer is the zero-effort option but cannot keep in-place ZC past the
16 KiB frame cap. The full transport is fastest but Go-only. The extension is
the *same engine* as the full transport plus a conformance profile, so a
foreign peer can interoperate at a cost that is bounded and, as §3.3 shows,
mostly recoverable by negotiation.

(A more academic middle option — adding generic byte-buffer hooks to gRPC-Go's
own framer so a plain `net.Conn` could approach the custom transport — would
require *new public gRPC-Go interfaces* and still not reach the full
transport's inline path. We did not pursue it: the custom transport already
exists and needs no core-API change.)

---

## Part 3 — Adapting the Go transport, and measured results

Part 2 described the extension at the design level. Part 3 is the concrete
Go-side adaptation — what changes in the existing transport — followed by the
measured cost and its analysis.

### 3.1 Adapting the Go transport

An audit of the current transport shows the gap to a conformant `CrossLangV1`
is small: the three hardest prerequisites are already satisfied, and the
remaining work is bounded.

| Item | Current state | Change for `CrossLangV1` | Status |
|---|---|---|---|
| `speculativeReserved@0x38` ABI field | `AddSpeculativeReserved` has zero call sites; the reader uses the interop-safe deferred-`readIdx` ZC; the writer's reads always subtract 0 | Retire the field (rename `reservedZero`, assert `== 0`); behaviour-preserving | **done** |
| Outbound frame size | Hard-wired to the Go-tuned `shmMaxFrameSize` | Per-connection override, set from negotiated `SETTINGS_MAX_FRAME_SIZE` | **done** (override mechanism; SETTINGS feed pending) |
| eventfd wake | Already the production default; futex is the fallback | Make eventfd mandatory; on setup failure fall back to the UDS bootstrap, never futex | pending |
| HTTP/2 data wire | Already H2-only (handshake advertises only H2, rejects non-H2 peers) | — | already conformant |
| SETTINGS | Validated then skipped — no state machine | Real state: send-first (no ACK-wait), ACK on receipt, enforce first H2 frame is SETTINGS, drive frame size + window from the peer's values | pending (next) |
| Flow control | Large 32 MiB window keeps WINDOW_UPDATE dormant | HTTP/2-default 64 KiB windows unless negotiated larger | pending (with SETTINGS) |
| `writeH2Single` > ring capacity | Rejects any frame larger than the ring | Incremental write for large HEADERS/CONTINUATION + `ringCap ≥ maxNonDataFrame` guard | pending |
| Liveness | No prompt `kill -9` detection (eventfd has no EOF) | UDS-EOF is the crash signal; segment `closed` flag is graceful close (drain first); bounded ring-wait | pending |

Two implementation notes:

- **SETTINGS is the keystone.** The window and frame size are useless as
  per-connection overrides until both ends agree on them in-band; an attempt
  to set a per-connection 64 KiB window *without* SETTINGS works for messages
  inside the window but stalls on larger messages, because the two ends never
  agree on the WINDOW_UPDATE threshold. Window negotiation therefore belongs
  in the SETTINGS step, not bolted on before it. (This was confirmed
  empirically — §3.3.)
- **SETTINGS ordering vs. the security handshake.** The optional security
  handshake runs on the raw rings *before* the H2 transport starts, so the
  "first frame must be SETTINGS" rule keys off the first H2 frame *after* that
  handshake, not byte 0 of the ring.

**Public API.** The adaptation adds **no new public type or function**. The
existing `experimental/shm` surface (`WithTransport`,
`WithTransportAndOptions`, `NewListener`, `DialOptions`, `ListenerConfig`)
gains one boolean on each of the two option structs:

```go
type DialOptions struct {     // already an alias of transport.DialOptions
    // ...
    CrossLangV1 bool   // request the cross-language-conformant profile
}
type ListenerConfig struct {
    // ...
    CrossLangV1 bool
}
```

The profile-negotiation bit travels on the existing CONNECT/ACCEPT flags byte
(internal wire). Default `false` = `GoPrivateV1`, so existing users are
byte-for-byte unaffected — fully backward compatible.


### 3.2 Measured cost

Because `CrossLangV1` remains a **custom transport**, it keeps the two things
the pure dialer (Part 1) structurally could not: it reserves header+payload
together in one contiguous ring slot (write ZC) and the receiver parses bytes
in place *within* each frame. It is **not** subject to the stock framer's
16 KiB-cap-plus-interleave ceiling that pinned the dialer at 22 % retention.

The strict-`CrossLangV1` per-RPC cost is **measurable today** via the global
flow-control knobs (`BENCH_PROFILE=fair-default` = HTTP/2-default 64 KiB
windows, `SHM_MAX_FRAME_SIZE=16384` = HTTP/2-default frame size), which impose
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
- **strict CrossLangV1 beats the pure-dialer floor.** At 64 KB, strict
  CrossLangV1 (126,806 ns) is faster than the pure `net.Conn` dialer
  (152,350 ns from Part 1), because it remains a custom transport — it keeps
  single-copy-into-ring ZC *within* each 16 KiB frame and full kernel bypass,
  both of which the dialer's `net.Conn` double-copy forfeits. This is the
  empirical payoff of the extension over the pure-dialer floor.

The remaining open question for the *real* cross-language number is whether a
real C-core / .NET / Java peer will accept a larger `SETTINGS_MAX_FRAME_SIZE`
and window. If it does, the cost shrinks toward the GoPrivateV1 baseline; if
it caps at 16 KiB / 64 KiB, the "strict" column above **is** the real-world
cross-language performance — still the best any cross-language local transport
can do.

The numbers above were obtained through the process-global flow-control knobs,
which impose the identical window + frame cadence the negotiated `CrossLangV1`
profile applies (so this measures the steady-state per-RPC cost without yet
requiring the SETTINGS step). A per-connection `CrossLangV1` option was
prototyped and is correct for messages within the window, but a robust profile
for messages *larger* than the window requires the SETTINGS handshake — see
§3.1.

### 3.3 Where the cost comes from — frame vs window, and why it is a floor not a tax

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
  ceiling. The whole job of the `CrossLangV1` SETTINGS step (§3.1) is
  to negotiate the frame size *up* to whatever the peer will accept. So the
  dominant 80 % of the conformance cost is exactly the part the extension's
  own negotiation can claw back.

**The honest reframing (important).** The "strict CrossLangV1" column in §3.2
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

### 3.4 Engineering cost — code size

The gRFC SHM transport is ~23,800 lines across 52 files (ring SPSC sync,
eventfd/futex wake, HPACK codec, HTTP/2 framing, flow control, the ZC fast
path + multi-anchor FIFO, security handshake, segment lifecycle, Linux +
Windows). The `CrossLangV1` extension on top of it is:

- **landed already** (committed): `speculativeReserved@0x38` retirement
  (net −54, dead-code removal) + per-connection outbound frame-size override
  (+45 −8) ≈ **~80 lines of new logic** — about **0.3 %** of the engine.
- **remaining** (estimated): SETTINGS state machine (~100 core / ~300 with
  ACK + first-frame + ordering), UDS bootstrap handshake (~200–250, reusing
  the existing per-segment fd-pass UDS), liveness (~120), hardening (~150),
  cross-language conformance tests (~300). **Core path ≈ 400–600 lines;**
  full hardened + tested ≈ ~1,300 lines — i.e. **~6 %** of the engine.

The extension is small *because* the gRFC engine already exists; it is a
negotiation layer, not a second transport, and adds no new public API (§3.1).
The genuinely large remaining cost of true interop is not in Go at all — it is
a *second language's* native reimplementation of the same spec (§2.2).

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
