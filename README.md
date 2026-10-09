# binance-lob-capture

C++17 program that connects to Binance's `depth@100ms`, `depth5@100ms` and `trade`
websocket streams, writes every message to a market data CSV, keeps a local order
book from those messages, and writes the top 5 levels to an order book CSV after
every event. It also has a replay mode that rebuilds the order book CSV from the
market data CSV without any network, and gets the exact same file.

## What's in this repo

| Path | What | |
|---|---|---|
| [`src/`](src), [`CMakeLists.txt`](CMakeLists.txt) | the program | **deliverable** |
| `README.md` (this file) | build, run, formats, policies, design note | **deliverable** |
| [`samples/`](samples) | 2-minute BTCUSDT spot run: both CSVs | **deliverable** |
| [`tests/`](tests) | 23 unit tests | extra |
| [`scripts/check_run.sh`](scripts/check_run.sh) | checks headers, column count, and that replay matches | extra |
| [`docs/design.md`](docs/design.md) | longer notes: what I measured, why it's built this way, bugs I hit | extra |

### Where each requirement is answered

| Assignment asks for | Where |
|---|---|
| compiler, C++ standard, dependencies, build commands | [Build](#build) |
| exact CLI to produce both CSVs | [Run](#run) |
| symbol list format | [Run](#run) |
| market data CSV columns, file naming | [Market data CSV](#market-data-csv-deliverable-a) |
| order book CSV header (26 columns), `id` / `type` / `side` mapping | [Order book CSV](#order-book-csv-deliverable-b) |
| scaling policy | [Price and size scaling](#price-and-size-scaling) |
| timestamp policy | [Timestamps](#timestamps) |
| reconnect / gap handling (`conn_epoch`, `U`/`u`/`pu`) | [Reconnects and gaps](#reconnects-and-gaps) |
| design note on threading and I/O | [Threading and I/O](#threading-and-io) |
| short sample run | [Sample run](#sample-run) and [`samples/`](samples) |
| replay mode with no network | [Replay](#replay-no-network) |

---

## Build

- **Language:** C++17
- **Compiler:** GCC 12 (`g++-12`) on Ubuntu 22.04. It also builds with GCC 11.4.
- **Dependencies:**
  - Boost 1.74 (Asio + Beast, header-only)
  - OpenSSL 3
  - simdjson 4.6.11, which CMake downloads and checks against a pinned SHA-256
- **Build tools:** CMake ≥ 3.16, Ninja

```bash
sudo apt-get update
sudo apt-get install -y cmake ninja-build g++-12 libssl-dev libboost-all-dev zlib1g-dev

cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-12
cmake --build build --parallel
```

- **Release build:** `-O3 -DNDEBUG` (CMake's default).
- **Warnings:** `-Wall -Wextra -Wpedantic -Wshadow -Wconversion` on all my code, and it
  builds with **zero warnings**.
- **`-march`:** CMake adds `-march=native` because simdjson picks its SIMD code at
  compile time. Without it you get the slow scalar version, and the program logs which
  one it got at startup. Use `-DBLC_MARCH=` for a portable binary.
- **Other options:** `-DBLC_SANITIZE=ON` (ASan + UBSan), `-DBLC_WERROR=ON`.

## Run

```bash
# one symbol, spot, 2 minutes
./build/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output --duration 120

# several symbols (one websocket connection, one pair of files per symbol)
./build/binance_capture --venue spot --symbols BTCUSDT,ETHUSDT --output-dir ./output

# USD-M futures
./build/binance_capture --venue usdm --symbols BTCUSDT --output-dir ./output
```

Without `--duration` it runs until Ctrl+C. Either way, it closes the websocket,
flushes the files and prints its counters.

| Option | Default | |
|---|---|---|
| `--venue spot\|usdm` | `spot` | spot: `wss://stream.binance.com:9443/stream`, usdm: `wss://fstream.binance.com/public/stream` |
| `--symbols LIST` | required | see below |
| `--output-dir DIR` | `./output` | created if missing |
| `--duration SECONDS` | `0` | 0 = until Ctrl+C |
| `--symbols-per-shard N` | `100` | symbols per websocket connection (3 streams each, Binance allows 1024, so max 341) |
| `--stats-interval SECONDS` | `10` | one stats line on stderr every N seconds, 0 = off |
| `--ws-url URL` | | other endpoint, e.g. `wss://stream.binance.com:443/stream` if port 9443 is blocked |
| `--replay FILE` | | replay mode, see [Replay](#replay-no-network) |

**Symbol list format:** comma separated with no spaces, and case doesn't matter
(`btcusdt,ETHUSDT`). Each symbol is 1–20 letters/digits, duplicates are ignored, and the
maximum is 200. Symbols are written in upper case in the CSVs.

**Output:** two files per symbol, named after the UTC date the run started (they don't
rotate at midnight; an existing file with the same name is overwritten):

```
output/market_data_spot_BTCUSDT_<UTC-date>.csv
output/market_data_spot_BTCUSDT_<UTC-date>_orderbook.csv
```

I went with one file per symbol instead of one file with a symbol column. That way each
order book file belongs to exactly one market data file, and replay can work one
instrument at a time.

Both files are UTF-8 with LF line endings and RFC 4180 quoting: a field that has a comma,
quote or newline is wrapped in `"` and inner quotes are doubled. The first row is the
header.

## Market data CSV (deliverable A)

```
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
```

| Column | |
|---|---|
| `recv_tsec`, `recv_tnsec` | receive time, see [Timestamps](#timestamps) |
| `venue` | `spot` or `usdm` |
| `stream_kind` | `depth_diff`, `depth5` or `trade` |
| `shard_id` | which websocket connection (0 if there's only one) |
| `conn_epoch` | 0 for the first connection, +1 every reconnect |
| `conn_seq` | 0, 1, 2… per connection. It counts every message on that connection, so with several symbols on one connection each file has gaps where the other symbols' messages went. |
| `symbol` | upper case |
| `payload_json` | the inner `data` object exactly as Binance sent it (already minified), CSV-quoted. It is not re-serialised. If a payload ever had whitespace, it would be minified (counted in `minified_payloads`). |

There is one row per message, in the order they were received. That is also the order the
order book processes them, because the row is written right before the book update.
Frames that can't be parsed or belong to a stream I didn't subscribe to get no row. They
are counted and logged instead.

## Order book CSV (deliverable B)

```
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

| Column | |
|---|---|
| `tsec`, `tnsec` | the receive time of the message that caused this row (same as its market data row) |
| `seqNo` | 1, 2, 3… one per row in this file |
| `id` | FNV-1a 32-bit hash of `"<venue>:<SYMBOL>"` with the sign bit cleared. It only depends on the instrument, so it's the same every run and in replay. |
| `bid0..4`, `ask0..4` | top 5 prices, best first, [scaled](#price-and-size-scaling) |
| `bid_size0..4`, `ask_size0..4` | sizes at those prices, same scale |

Empty levels (fewer than 5 on a side) are `0`.

**When rows are written, and the `type` / `side` values:**

| `type` | written when | `side` |
|---|---|---|
| `D` | a depthUpdate was applied | `B` only bids changed, `S` only asks changed, `N` both |
| `P` | a depth5 seeded the book, or checked it at the book's own update id | `N` |
| `T` | a trade came in while the book was live. **The book isn't changed**: trades reach the book through the diffs, so this row only shows the book at that moment. | side of the book that got hit: `m=true` (buyer is maker, so a seller hit the bid) → `B`, `m=false` → `S` |
| `R` | the book was dropped because of a gap or a reconnect; all prices and sizes are 0 | `N` |

Nothing is written while the book is waiting for a snapshot. Events that don't produce a
row are counted (`diffs_buffered`, `diffs_stale`, `depth5_stale`, ...).

## Price and size scaling

**Prices and sizes both use a fixed scale of 10^8.**
- `"62000.10"` → `6200010000000`
- `"0.00030000"` → `30000`
- To get the real number back, divide by 100,000,000.

[`parseScaled()`](src/decimal.hpp) reads the decimal string straight into an `int64`: the
digits before the dot, then up to 8 after it, padded with zeros. There's no `double`
anywhere between the socket and the CSV. Binance spot has at most 8 decimals (futures has
fewer), so every value is exact.

A value that would need rounding is rejected and counted under `decode_errors`, and it
doesn't touch the book. That covers:
- a non-zero 9th decimal;
- a minus sign or an exponent;
- anything over 92,233,720,368.54775807 (the int64 limit at this scale).

## Timestamps

**Wall-clock time (`system_clock`, nanoseconds) at the moment the whole websocket message
has been read.** It's taken first thing in the read handler and split into `recv_tsec` and
`recv_tnsec` (0…999,999,999). The order book row made from that message gets the same
timestamp.

Since the time is saved in the market data CSV, replay gives exactly the same timestamps.
Binance's own event times (`E`, `T`, in ms) are still inside `payload_json`.

## Reconnects and gaps

Short version; [docs/design.md](docs/design.md#2-keeping-the-book-in-sync) has the full
logic and why it's correct.

**Startup / after a gap / after a reconnect**
- Diffs are buffered.
- The next `depth5` seeds the book: buffered diffs it already covers are dropped, and the
  rest are applied on top.
- If the buffer doesn't connect to the snapshot, I wait for the next `depth5` (they come
  every 100 ms).

**Sequence check on every diff**
- Spot: `U ≤ last_u + 1`.
- Futures: `pu == last_u`.
- Diffs with `u ≤ last_u` are old and dropped.
- If the check fails, it's a gap: the book is dropped (`R` row) and re-seeded from the
  next `depth5`.

**New `conn_epoch`** (any reconnect)
- I can't know what I missed, so the book is dropped (`R` row) and the buffer cleared.
- It re-seeds from the new connection's first `depth5`.
- Reconnects use exponential backoff: 0.5 s, then up to 30 s.

**`depth5` while the book is live** checks and fixes the top 5. Before writing this I
recorded both venues to see how `depth5` lines up with the diffs:
- On spot it usually arrives **one message before** the diff it matches.
- On futures its id falls **inside** a diff's range.

So I hold a `depth5` that's ahead of the book and apply it right before the diff that
covers its id. Every check is counted. Across all my test runs, `levels_mismatched`
(snapshot disagreed with a level the book was sure about) was **0**.

## Threading and I/O

- **One thread does everything.**
  - One `io_context` runs the socket reads, JSON parsing, book updates and CSV
    formatting.
  - Nothing is shared between threads, so there are no locks or atomics.
  - One symbol is around 30–100 messages per second and each takes roughly 5–20 µs
    (median ≤ 16 µs in the sample run), so the thread is idle more than 99% of the time.
    A writer thread would only add a queue hop.
- **Buffered writes.**
  - Each file has a 256 KB buffer, written with one `fwrite` when it fills up and every
    1 s from a timer (so a crash loses about a second at most).
  - That's one small write every few seconds, so the network thread never really waits
    on the disk.
- **Back-pressure.**
  - If processing ever fell behind, data would sit in the kernel socket buffer and TCP
    would push back on Binance. Binance eventually drops slow clients, which shows up as
    a counted reconnect.
  - Nothing in memory grows without a limit: buffered diffs ≤ 256, a message ≤ 4 MB, a
    book side ≤ 6000 levels.
- **Order book.** Each side is a sorted `std::vector` with the best price at the back.
  - Updates near the top are cheap and reading the top 5 is O(1).
  - There's no allocation per level like with `std::map`.
  - The trade-off is explained in
    [docs/design.md](docs/design.md#4-order-book-data-structure).
- **No allocations in steady state.** Message processing does no heap allocation once
  warmed up, and a unit test checks this.

## Sample run

[`samples/`](samples) has a 2-minute capture of BTCUSDT spot made with:

```bash
./build/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output --duration 120
```

| File | Rows | Size |
|---|---|---|
| [`market_data_spot_BTCUSDT_2026-10-07.csv`](samples/market_data_spot_BTCUSDT_2026-10-07.csv) | 3538 (1162 depth_diff, 1162 depth5, 1214 trade) | 1.6 MB |
| [`market_data_spot_BTCUSDT_2026-10-07_orderbook.csv`](samples/market_data_spot_BTCUSDT_2026-10-07_orderbook.csv) | 2376 (1 P, 1161 D, 1214 T) | 0.6 MB |

Counters printed at the end of that run:

```
[live] transport: connects=1 disconnects=0 messages=3538 bytes=1347972 envelope_errors=0 unknown_streams=0 minified_payloads=0
[live] book: diffs_applied=1161 diffs_stale=1 diffs_buffered=0 buffer_overflows=0 gaps=0 epoch_resets=0 seeds=1 seed_failures=0 decode_errors=0
[live] depth5: checks=1161 stale=0 superseded=0 levels_mismatched=0 levels_learned=3 | trades=1214 uncertain_rows=0
[live] output: market_rows=3538 book_rows=2376 write_errors=0
[live] latency (read->rows, ns): n=3538 mean=16387 p50<=16384 p99<=131072 max=610160
```

- **Gaps:** none.
- **depth5 checks:** every one of the 1161 `depth5` messages was checked against the book,
  and none disagreed.
- **Diffs:** the 1 stale diff is the one covered by the seeding snapshot.
- **P rows:** there's only one, from the seed. Every other `depth5` arrived just before its
  matching diff, so it was applied together with that diff (its `D` row).

First rows of each file (the payload is cut short here):

```
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
1791395834,638904591,spot,depth5,0,0,0,BTCUSDT,"{""lastUpdateId"":101158657882,""bids"":[[""83187.53000000"",""0.92962000""],...
1791395834,638926654,spot,depth_diff,0,0,1,BTCUSDT,"{""e"":""depthUpdate"",""E"":1791395834614,""s"":""BTCUSDT"",""U"":101158657871,""u"":101158657882,...
```

```
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
1791395834,638904591,1,2014473836,P,N,8318753000000,8318752000000,8318751000000,8318722000000,8318656000000,92962000,62000,13000,8000,7000,8318754000000,8318755000000,8318756000000,8318760000000,8318773000000,916138000,5020000,13000,5007000,4000000
1791395835,161053848,2,2014473836,D,N,8318753000000,8318752000000,8318751000000,8318722000000,8318656000000,92962000,62000,13000,8000,7000,8318754000000,8318755000000,8318756000000,8318760000000,8318773000000,916138000,5020000,13000,5007000,4000000
```

`8318753000000` = 83187.53000000 USDT.

- **The first diff** ends at exactly the snapshot's id (`u = 101158657882`), so the
  snapshot already contains it. It's dropped (that's the `diffs_stale=1`) and gets no
  order book row.
- **Order book row 2** is the next diff (see its later timestamp). It only changed levels
  below the top 5.

I also ran USD-M, two symbols at once, and a forced disconnect. The results are in
[docs/design.md](docs/design.md#9-numbers).

## Replay (no network)

```bash
./build/binance_capture --replay samples/market_data_spot_BTCUSDT_2026-10-07.csv --output-dir ./replay
cmp samples/market_data_spot_BTCUSDT_2026-10-07_orderbook.csv replay/market_data_spot_BTCUSDT_2026-10-07_orderbook.csv
```

Replay reads only the market data CSV, makes no network calls, and goes through the same
code as the live run. The order book file it writes is **byte-for-byte identical** to the
live one.

[`scripts/check_run.sh`](scripts/check_run.sh) does the header check, the 26-column check
and this replay comparison in one go.

## Extras

These go beyond what was asked:
- **Unit tests:** `cmake --build build --target tests && ./build/tests/unit_tests` (23
  tests, see [docs/design.md](docs/design.md#10-tests)).
- **Sanitizer build:** clean under ASan + UBSan.
- **Counters and latency:** printed every 10 s and at exit.
- **Sharding:** several connections for many symbols.
- **depth5 cross-check:** the book is checked against every `depth5`.
- **Measurements and bug notes:** what I measured and the bugs I found while testing are
  in [docs/design.md](docs/design.md).

Not done: the REST snapshot stretch goal. The reason and how I'd add it are in
[docs/design.md](docs/design.md#11-what-id-do-next).