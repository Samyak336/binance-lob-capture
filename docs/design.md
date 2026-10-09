# Design notes (extra)

The [README](../README.md) has everything the assignment asks for. This page
is the longer version: what I measured before writing the sync logic, why the
code is shaped the way it is, and what broke while I was testing it.

- [1. Looking at the streams first](#1-looking-at-the-streams-first)
- [2. Keeping the book in sync](#2-keeping-the-book-in-sync)
- [3. How much of the book do we actually know?](#3-how-much-of-the-book-do-we-actually-know)
- [4. Order book data structure](#4-order-book-data-structure)
- [5. Parsing and allocations](#5-parsing-and-allocations)
- [6. Threading, I/O, back-pressure](#6-threading-io-back-pressure)
- [7. Errors, shutdown, limits](#7-errors-shutdown-limits)
- [8. Things that broke while testing](#8-things-that-broke-while-testing)
- [9. Numbers](#9-numbers)
- [10. Tests](#10-tests)
- [11. What I'd do next](#11-what-id-do-next)

---

## 1. Looking at the streams first

Before writing any book logic, I recorded 30 s of each venue (BTCUSDT, Oct 2026)
with a throwaway script and checked how `depth5` lines up with the diffs.

| | spot | USD-M |
|---|---|---|
| diff sequence | `U == prev u + 1` every time | `pu == prev u` every time |
| `depth5` id equals some diff's `u` | always | 5 of 287 |
| `depth5` id strictly inside a diff's `[U, u]` | never | 282 of 287 |
| `depth5` arrives **before** the diff ending at its id | 283 of 300 (94%) | — |

Two things surprised me:

- **On spot, `depth5` is usually one message ahead of its diff.** If you only
  apply `depth5` when its id equals the book's last update id, you skip almost
  all of them. Applying it blindly is worse: for one message the top of the
  book jumps ahead of everything below it.
- **On USD-M, `depth5` ids are in the middle of a diff's range.** USD-M
  `depth5` messages also say `"e":"depthUpdate"`, exactly like diffs. The
  stream name is the only way to tell them apart, so the router decides the
  kind from the stream name, never from the payload.

Also: the spot `depth5` payload has no symbol at all, so the symbol always
comes from the stream name too.

## 2. Keeping the book in sync

All of this lives in [src/book_sync.cpp](../src/book_sync.cpp).

**Not live** (startup, after a gap, after a reconnect)
- Diffs go into a 256-slot ring buffer.
- The next `depth5` (id `L`) seeds the book. Buffered diffs with `u ≤ L`
  are thrown away, and the rest are applied with the normal checks.
- If the first remaining diff doesn't connect (spot `U > L+1`, futures
  `pu > L`), the snapshot is older than the buffer. I wait for the next one.

**Live**
- Diff with `u ≤ last_u`: old, drop it.
- Continuity check:
  - spot: `U ≤ last_u + 1`;
  - futures: `pu == last_u`;
  - futures right after a seed: `pu ≤ L < u` is enough.
- Anything else is a **gap**: I throw the book away, write an `R` row,
  buffer this diff and wait for the next `depth5` (≤ ~100 ms away).

**`depth5` while live**, with id `L`:
- `L == last_u`: compare it with the book, then overwrite the top region
  (`P` row).
- `L < last_u`: old, ignore it.
- `L > last_u`: hold it. Right before applying the diff with
  `last_u < L ≤ u`, lay the snapshot on the book, then apply the diff.

Why holding is correct: a diff carries the **final** quantity (as of `u`) for
every level that changed anywhere in `(last_u, u]`.
- Levels the diff touches get overwritten with their value at `u` anyway.
- Levels it doesn't touch didn't change between `L` and `u`, so the
  snapshot's values for them are already current.

When comparing snapshot and book I skip the prices the diff is about to
overwrite. Anything else that differs is a real disagreement.

**Reconnect (new `conn_epoch`)**
- I can't know what I missed while disconnected, so the book on that shard
  is dropped (`R` row) and the buffer cleared.
- It re-seeds from the new connection. Nothing from the old connection is
  mixed in.

All of these decisions only look at what's in the market data csv (payload,
stream kind, `conn_epoch`). That's what makes replay give the exact same
output.

## 3. How much of the book do we actually know?

A `depth5` tells me exactly which levels exist down to its 5th price. Below
that, I only know about levels that a diff happened to touch. Each book side
keeps that boundary (`verifiedEdge_`):

- Diffs keep everything above the edge exact.
- Each new snapshot pushes the edge further down.

That gives two counters:

- **`levels_mismatched`**: a snapshot disagreed with the book *above* the
  edge. If the sequencing logic is right this stays at 0, and it has been 0 in
  every run.
- **`uncertain_rows`**: rows where one of the 5 slots is below the edge. For
  example, the best bids get cancelled and a level no snapshot has shown yet
  moves into the top 5. The row is written anyway, and the next `depth5` fills
  the hole (`levels_learned`). These rows are rare.

A REST snapshot (5000 levels) at startup would make the edge 5000 levels deep;
see [what I'd do next](#11-what-id-do-next).

## 4. Order book data structure

Each side is a `std::vector<Level>` sorted worst → best, so **the best price is
at the back** ([src/order_book.hpp](../src/order_book.hpp)).

| | sorted vector (mine) | `std::map` |
|---|---|---|
| find a level | binary search, O(log n) | O(log n) |
| insert/erase | shifts the elements between it and the back | O(log n) + a heap node alloc/free |
| top 5 | last 5 elements, O(1) | walk 5 nodes |
| memory | contiguous, 16 B per level | node per level, pointer chasing |

Almost every update is near the touch, which is the back of the vector, so
inserts and erases move only a few elements. With diffs carrying dozens to
hundreds of levels, a map would allocate and free nodes hundreds of times a
second per symbol.

The worst case is an insert far from the touch, which shifts the whole
vector. To bound that and the memory, a side keeps at most 5000 levels.
When it reaches 6000, the 1000 worst are dropped in one go (one memmove,
amortised), and the verified edge moves up to the new worst level.

I also thought about a tick-indexed array around the mid price (O(1)
updates). But it needs each symbol's tick size and recentring when the price
moves, which isn't worth it at this message rate.

## 5. Parsing and allocations

**JSON.** I used simdjson's On-Demand API, and each frame is parsed once:

1. read `"stream"`;
2. grab the raw bytes of `"data"` with `raw_json()`. That's `payload_json`
   as-is, with no re-serialising, so numbers and strings stay exactly as
   Binance sent them;
3. `reset()` the same object and decode its fields in one pass.

Replay parses each payload once from the csv.

The gotcha: On-Demand picks its SIMD implementation **at compile time**.
Without `-march`, x86-64 gets the scalar "fallback" code. I only noticed
because a compile error mentioned `simdjson::fallback::ondemand`. So CMake
passes `-march=native` by default, and the program logs the kernel it got
at startup (`simdjson kernel: haswell`).

simdjson needs 64 readable bytes after the input and Beast's buffer doesn't
promise that, so each frame is copied once into a padded buffer (~2 KB
memcpy).

**Allocations.** Processing a message does **zero heap allocations** once
warmed up. There's a test that replaces `operator new` with a counter, runs
3000+ mixed messages and checks the count didn't move. These are all reused:

- the Beast buffer and the padded frame buffer;
- the simdjson parser (preallocated to 1 MB);
- the decoded diff's vectors;
- the book vectors (reserved up front);
- the csv buffers;
- the ring buffer slots.

## 6. Threading, I/O, back-pressure

**Single thread.** One `io_context` does socket reads, parsing, book updates
and csv formatting for every connection.
- With nothing shared between threads, there are no locks, no atomics and no
  data races to reason about.
- One symbol is ~30–100 messages/s and a message takes roughly 5–20 µs, so the
  thread is idle >99% of the time.
- A writer thread would add a queue hop and cross-core cache traffic without
  removing anything that actually blocks.

**Batched writes.** Each output file has its own buffer:
- Rows go into it via `std::to_chars` (no locale, no allocation).
- It's written with one `fwrite` per 256 KB, plus every second from a timer,
  so a crash loses at most ~1 s.
- stdio buffering is switched off (`_IONBF`) since I already buffer.

A 256 KB write into the page cache is tens of microseconds, every few
seconds.

**Back-pressure.** If processing ever fell behind, data would wait in the
kernel's socket buffer, and TCP flow control would push back on Binance (which
eventually drops slow clients). That would show up as a counted reconnect,
not as memory growing. Nothing in user space grows without bound:

| What | Limit |
|---|---|
| output buffers | flushed at a fixed size |
| buffered diffs | 256 |
| one message | 4 MB |
| book side | 6000 levels |

**If this had to scale:**
- Shards (connections) already don't share anything, so the next step would
  be one `io_context` per core with shards spread across them. That's still
  lock-free.
- If disk latency ever showed up (say, network storage), I'd move `fwrite`
  to a writer thread fed by a single-producer/single-consumer queue of filled
  buffers.

## 7. Errors, shutdown, limits

| what goes wrong | what happens |
|---|---|
| resolve / connect / TLS / handshake / read error, or 30 s with no data (Beast pings at 15 s) | logged; reconnect with backoff 0.5 s → 30 s; new `conn_epoch`; leftover partial message discarded |
| bad envelope, unknown stream | no row; `envelope_errors` / `unknown_streams`; first 20 logged |
| bad payload (json, missing field, decimal, malformed levels, `U > u`) | market data row is still written (it's an audit log), book untouched, `decode_errors` |
| `depth5` not best-first, more than 5 levels, or qty ≤ 0 | treated as a bad payload |
| file write fails | `write_errors`, exit code 3 |
| replay hits a malformed row | logged with the record number, exit code 3 |

**Shutdown.** Ctrl+C, `SIGTERM` or `--duration` does this:
1. Timers are cancelled.
2. Each connection sends a websocket close frame and **keeps reading** until
   Binance answers (Beast requires that; see §8), with a 2 s limit.
3. `io_context::run()` returns.
4. Files are flushed and closed by their owners, and the metrics are printed.

A second Ctrl+C exits right away (files are still flushed).

**Ownership.** Every resource has one owner:
- files: `unique_ptr<FILE>` with an `fclose` deleter;
- the websocket: `std::optional` (re-created on each reconnect);
- each connection is kept alive by `shared_from_this()` until its last async
  handler has run.

**Security.**
- TLS verifies the certificate chain (system CA store), sends SNI, and checks
  the hostname (`ssl::host_name_verification`).
- No API keys at all; it's public market data only.
- Limits: ≤ 200 symbols, ≤ 341 per connection (Binance allows 1024 streams),
  URL ≤ 8 KB, plus the buffer limits above.

## 8. Things that broke while testing

These all came out of actually running the program, and fixing them changed
the code:

1. **A half-received message glued onto the next connection.** To test
   reconnects I froze the process with `SIGSTOP` for 75 s so Binance would
   drop it. After the reconnect, the first frame was garbage:
   `{"stream":"btcusdt@trade","data":{"e{"stream":...`. A read that dies
   mid-message leaves the partial bytes in Beast's buffer, and I was reusing
   the buffer. The parser rejected the frame (so no bad data got through), but
   a real message was lost. Fix: empty the buffer at the start of every
   connection.
2. **Shutdown took ~11 s.**
   - First cause: after sending the close frame I stopped reading. Beast's
     docs say you have to keep reading until the close comes back.
   - Second cause: Binance is sometimes slow to answer, and Beast's
     close timeout is the 10 s handshake timeout. I now drop it to 2 s right
     before closing.
   - Shutdown is now under a second, two at worst.
3. **Ctrl+C could hang forever.** In the signal handler I re-armed the signal
   wait *after* starting shutdown. If every connection stopped immediately,
   shutdown cancelled the old wait, and then my new wait kept `io_context`
   alive forever. I found this by reading the code, not by hitting it. Fix:
   re-arm first.
4. **The scalar simdjson kernel** (§5).
5. **The sample csvs would have been git-ignored.** The provided `.gitignore`
   has `*.csv`, which also excludes the sample run. I added `!samples/*.csv`.

## 9. Numbers

From live runs on my laptop (WSL2, Ubuntu 22.04, `-O3 -march=native`, GCC 12).
"Latency" is the time from the socket read finishing to both csv rows being
formatted.

| run | messages | diffs applied | gaps | depth5 checks | mismatched levels | latency p50 | replay identical |
|---|---|---|---|---|---|---|---|
| spot BTCUSDT, 60 s | 2832 | 507 | 0 | 507 | 0 | ≤ 8 µs | yes |
| USD-M BTCUSDT, 45 s | 1082 | 293 | 0 | 274 | 0 | ≤ 16 µs | yes |
| spot BTCUSDT + ETHUSDT, 45 s | 3477 | 800 | 0 | 800 | 0 | ≤ 8 µs | yes (both) |
| spot BTCUSDT, frozen 75 s to force a reconnect | 3458 | 555 | 0 | 555 | 0 | ≤ 2 µs | yes |

In the reconnect run: Binance dropped the connection, I reconnected 0.5 s
later with `conn_epoch=1`, wrote one `R` row and re-seeded. The two-minute
run in `samples/` is in the README.

USD-M is slower per message because its diffs are much bigger (hundreds of
levels). The latency histogram uses power-of-two buckets, so "≤ 8 µs" means
the median falls in the 4–8 µs bucket.

## 10. Tests

`./build/tests/unit_tests` runs 23 tests with no framework
([tests/unit_tests.cpp](../tests/unit_tests.cpp)):

- **Decimal parsing:** exact values, the int64 limit, and rejecting
  over-precise, negative, exponent and overflowing input.
- **CSV:** RFC 4180 escaping; write → read round trip with quotes, commas and
  newlines inside fields; CRLF; broken quoting.
- **Book side:** ordering, qty 0 removes, removing a missing level, trimming,
  and the verified edge.
- **Spot sync:** seeding, buffered diffs older/newer than the snapshot, the
  snapshot being too old, gap → `R`, a held `depth5` applied with its diff,
  and catching a wrong level.
- **Futures sync:** `pu` chaining, a snapshot inside a diff's range, gaps.
- **Reconnect:** a new epoch drops the book.
- **Router end to end:** both csv formats checked byte for byte, unknown
  streams, garbage frames, minifying a payload with spaces.
- **Replay:** gives the same order book byte for byte across a gap and a
  reconnect.
- **Allocations:** zero in steady state.
- **CLI** parsing and limits.

The sanitizer build (`-DBLC_SANITIZE=ON`, ASan + UBSan) runs the tests and a
live capture cleanly.

## 11. What I'd do next

- **REST snapshot sync** (the stretch goal). This is Binance's documented
  procedure: buffer diffs, fetch `/api/v3/depth?limit=5000`, drop `u ≤
  lastUpdateId`, apply the rest. `seed()` already takes a snapshot and replays
  the buffer, so most of it is there.
  The catch is replay: to stay offline, the snapshot has to be saved in the
  market data csv as another `stream_kind`, which goes beyond the column
  contract. That's why I left it out.
- Rotate output files at UTC midnight (right now the name uses the start date).
- One `io_context` per core if there were hundreds of symbols.