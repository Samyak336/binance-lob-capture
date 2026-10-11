# binance-lob-capture

A C++17 program that records Binance's `depth@100ms`, `depth5@100ms` and `trade` streams,
keeps a local order book from them, and writes two CSV files per symbol: every websocket
message (`market_data_*.csv`) and the top 5 levels of the book after every event
(`*_orderbook.csv`).

The book starts from a REST depth snapshot plus buffered diffs, the way Binance's
"how to manage a local order book" guide describes, and every `depth5` message is used to
check it. There's also a replay mode that rebuilds the order book file from the saved files
with no network. In every run I've done, the replayed file was byte-for-byte identical to
the one written live.

If you only have a few minutes, [src/book_sync.cpp](src/book_sync.cpp) (update ids, gaps,
reconnects, depth5) and [src/order_book.hpp](src/order_book.hpp) (the book itself) are the
files I'd look at, and [How I tested it](#how-i-tested-it) has the numbers.

## Building

Ubuntu 22.04, GCC 12.3 (`g++-12`), C++17. It also builds with the default GCC 11.4.

Dependencies: Boost 1.74 (only Asio and Beast, both header-only), OpenSSL 3, and
simdjson 4.6.11, which CMake downloads itself and checks against a pinned SHA-256. On a
machine that can't download it, unpack the
[v4.6.11 release](https://github.com/simdjson/simdjson/archive/refs/tags/v4.6.11.tar.gz)
somewhere and add `-DFETCHCONTENT_SOURCE_DIR_SIMDJSON=/path/to/simdjson-4.6.11` to the
first cmake command.

```bash
sudo apt-get install -y cmake ninja-build g++-12 libssl-dev libboost-all-dev
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-12
cmake --build build

# unit tests
cmake --build build --target tests && ./build/tests/unit_tests
```

Release means `-O3 -DNDEBUG`. My code is compiled with
`-Wall -Wextra -Wpedantic -Wshadow -Wconversion` and builds without warnings, also with
the assignment's `-DCMAKE_CXX_FLAGS="-Wall -Wextra -O2"` (`-DBLC_WERROR=ON` turns warnings
into errors). `-DBLC_SANITIZE=ON` gives an ASan + UBSan build.

One flag that isn't obvious: CMake adds `-march=native` by default. simdjson picks its SIMD
code at compile time, and without `-march` you silently get its slow scalar fallback. I only
noticed because a compiler error mentioned `simdjson::fallback`. The program logs which
version it got at startup (`simdjson kernel: haswell`). If the binary has to run on a
different machine, configure with `-DBLC_MARCH=` (empty).

## Running

```bash
# BTCUSDT spot for 2 minutes (leave out --duration to run until Ctrl+C)
./build/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output --duration 120

# several symbols
./build/binance_capture --venue spot --symbols BTCUSDT,ETHUSDT --output-dir ./output

# USD-M futures
./build/binance_capture --venue usdm --symbols BTCUSDT --output-dir ./output
```

Symbols are a comma-separated list with no spaces, in any case (`btcusdt,ETHUSDT`). Each
one has to be 1-20 letters or digits, duplicates are dropped, and there can be at most 200.
Ctrl+C, SIGTERM or the end of `--duration` closes the websockets, flushes the files and
prints the counters. The exit code is 0 if everything went fine, 1 if a file couldn't be
opened or created, 2 for bad arguments, 3 if a write failed (or replay found malformed
rows), and 4 if it never managed to connect.

| Option | Default | |
|---|---|---|
| `--venue spot\|usdm` | `spot` | `wss://stream.binance.com:9443/stream` or `wss://fstream.binance.com/public/stream` |
| `--symbols LIST` | required | |
| `--output-dir DIR` | `./output` | created if missing |
| `--duration SEC` | `0` | 0 = until Ctrl+C |
| `--snapshot rest\|depth5` | `rest` | what a new book starts from, see [below](#keeping-the-book-in-sync) |
| `--snapshot-depth N` | spot 5000, usdm 1000 | levels in the REST snapshot |
| `--symbols-per-shard N` | `100` | symbols per websocket connection (3 streams each, Binance allows 1024) |
| `--stats-interval SEC` | `10` | a stats line on stderr every N seconds, 0 = off |
| `--ws-url URL` | | another endpoint, e.g. `wss://stream.binance.com:443/stream` if 9443 is blocked |
| `--replay FILE` | | [replay mode](#replay) |

Each symbol gets its own files, named after the UTC date the run started:

```
output/market_data_spot_BTCUSDT_2026-10-11.csv              every websocket message
output/market_data_spot_BTCUSDT_2026-10-11_orderbook.csv    top 5 after every event
output/market_data_spot_BTCUSDT_2026-10-11_snapshots.csv    the REST snapshots, so replay works offline
```

The assignment allowed one file per run with a symbol column instead. I went with one set
per symbol so that every order book file belongs to exactly one market data file, and
replay can work one instrument at a time. Files don't rotate at midnight, and a file with
the same name gets overwritten.

All files are UTF-8 with LF line endings and RFC 4180 quoting: a field containing a comma,
quote or newline is wrapped in `"` and quotes inside it are doubled. The first row is the
header.

## Market data CSV

```
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
```

There's one row per websocket message, in the order they were read. Each row is written just
before its message goes to the order book, so this is also the order the book sees.

| Column | |
|---|---|
| `recv_tsec`, `recv_tnsec` | receive time, see [timestamps](#prices-sizes-and-timestamps) |
| `venue` | `spot` or `usdm` |
| `stream_kind` | `depth_diff`, `depth5` or `trade`, taken from the stream name, never from the payload (USD-M depth5 messages say `"e":"depthUpdate"` just like diffs, and spot depth5 doesn't even contain the symbol) |
| `shard_id` | which websocket connection, 0 if there's only one |
| `conn_epoch` | 0 for the first connection, +1 on every reconnect |
| `conn_seq` | 0, 1, 2… per connection. It counts every message on that connection, so with several symbols on one connection, each file skips the numbers that went to the other symbols |
| `symbol` | upper case |
| `payload_json` | the inner `data` object exactly as Binance sent it. It's sliced out of the frame rather than re-serialised, and Binance already sends it minified (if it ever had whitespace it would be minified, counted as `minified_payloads`) |

A frame that isn't valid JSON, or comes from a stream I didn't subscribe to, has no symbol
to file it under. It gets no row and is counted instead (`envelope_errors`,
`unknown_streams`, with the first 20 logged). A payload that parses but can't be decoded
(a bad decimal, a missing field) still gets its row, because this file is the audit log,
but it doesn't touch the book (`decode_errors`).

## Order book CSV

The header is the one from the assignment, 26 columns:

```
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
```

| Column | |
|---|---|
| `tsec`, `tnsec` | receive time of the message that caused the row, the same value as in its market data row. Rows caused by a REST answer (the `S` row and the `D` rows of the buffered diffs applied with it) use the time the answer arrived, and a held depth5's `P` row uses the time of the diff it was applied with, so the column never goes backwards |
| `seqNo` | 1, 2, 3… per file |
| `id` | 32-bit FNV-1a hash of `"<venue>:<SYMBOL>"` with the sign bit cleared (spot BTCUSDT is `2014473836`). It only depends on the instrument, so it's the same in every run, in replay, and whatever order the symbols are listed in |
| `bid0..4`, `ask0..4` | top 5 prices, best first, [scaled](#prices-sizes-and-timestamps). Missing levels are `0` |
| `bid_size0..4`, `ask_size0..4` | sizes at those prices, same scale |

`type` and `side`:

| `type` | written when | `side` |
|---|---|---|
| `S` | a REST snapshot started the book. The buffered diffs applied on top of it follow as their own `D` rows | `N` |
| `D` | a depthUpdate was applied | `B` only bids changed, `S` only asks changed, `N` both |
| `P` | a depth5 was applied: checked against the book and written over its top 5 (or, with `--snapshot depth5`, used to start the book). The row shows the top 5 as of the depth5's update id | `N` |
| `T` | a trade came in while the book was live. **The book isn't changed by it**, the row just shows the book at that moment (see [trades](#keeping-the-book-in-sync)) | side of the book that got hit: `m=true` means the buyer was the maker, so a seller hit the bid → `B`; `m=false` → `S` |
| `R` | the book was thrown away because of a gap or a reconnect. All prices and sizes are `0` | `N` |

So every applied diff has its own `D` row and every applied depth5 its own `P` row, and the
counters printed at the end say how many of each there should be: `diffs_applied` and
`checks` (plus `seeds` with `--snapshot depth5`). A depth5 that's older than the book,
arrives while the book is waiting for its snapshot, or is replaced by a newer one before its
diff shows up is never applied, so it gets no row; those are counted as `stale`, `ignored`
and `superseded`. While a book is waiting for its snapshot, nothing is written for it.

## Prices, sizes and timestamps

**Scaling.** Prices and sizes use one fixed scale for every symbol, 10^8:
`"62000.10"` → `6200010000000`, `"0.00030000"` → `30000`. Divide by 100,000,000 to get the
decimal back.

[`parseScaled()`](src/decimal.hpp) reads the decimal string digit by digit straight into an
`int64`; there's no `double` anywhere between the socket and the CSV. Binance never sends
more than 8 decimals, so every value comes out exact. Anything that would need rounding is
rejected rather than rounded (counted under `decode_errors`, book untouched): a non-zero 9th
decimal, a sign or an exponent, or a value above 92,233,720,368.54775807, which is the
int64 limit at this scale.

**Timestamps.** Wall-clock time (`system_clock`, nanoseconds) taken at the moment the whole
websocket message has been read, split into seconds and nanoseconds (0…999,999,999). The
order book row made from that message gets the same timestamp. Since it's stored in the
market data file, replay reproduces it exactly. Binance's own event times (`E`, `T`, in
milliseconds) are still in `payload_json` if you want them.

## Keeping the book in sync

This is all in [src/book_sync.cpp](src/book_sync.cpp).

**How a book starts.** By default it follows Binance's procedure:

1. Buffer the diffs coming in on the websocket (up to 256).
2. When the first one arrives, fetch a REST snapshot: `/api/v3/depth?limit=5000` on spot,
   `/fapi/v1/depth?limit=1000` on USD-M (1000 is the futures maximum).
3. Drop the buffered diffs it already covers (`u <= lastUpdateId`).
4. If the first remaining diff doesn't connect to the snapshot, the snapshot is older than
   the buffer. Throw it away and fetch another one.
5. Otherwise start the book from the snapshot (`S` row) and apply the remaining buffered
   diffs on top, with the usual sequence check, each with its own `D` row.

depth5 messages that arrive while a book is waiting are ignored (counted as `ignored`).

The REST calls go out one at a time from the same thread as everything else, so a snapshot
always lands between two websocket messages. Binance limits request weight per IP per minute
(a 5000-level spot snapshot costs 250 of the 6000). The client reads the
`X-MBX-USED-WEIGHT-1M` header that comes back with every answer, waits for the next minute
once it's past half the limit, and honours `Retry-After` on a 429 or 418. If you start a lot
of spot symbols at once, `--snapshot-depth 1000` (weight 50) gets them going faster. A failed
request is logged and retried with backoff (1 s up to 30 s), and the book stays empty until
one succeeds. If the REST API isn't reachable from your network at all, use
`--snapshot depth5`.

`--snapshot depth5` starts the book from the next depth5 message instead, with no REST calls.
The first version of this program worked that way, and I kept it as an option because it's
a useful comparison. A depth5 only shows 5 levels, so right after starting the book
doesn't know anything below them. The book keeps track of how deep it knows the market for
sure (`verifiedEdge_` in [src/order_book.hpp](src/order_book.hpp)): if the best levels get
cancelled and a level it has never seen moves into the top 5, the row is counted under
`uncertain_rows`, and the next depth5 fills in the hole (`levels_learned`). How often that
happens depends on how busy the market is. On a busy 8 Oct, a 20-second depth5-mode run
had 13 and a 2-minute capture replayed that way had 35. On the quiet morning of 11 Oct the
same two numbers were 0 and 2. With the REST snapshot it has been 0 in every run.

**Sequence checks while live.** Every diff has to continue from the last one:

- spot: `U <= last_u + 1` (normally exactly `last_u + 1`)
- USD-M: `pu == last_u`. Right after a snapshot, the first diff only has to straddle it
  (`pu <= lastUpdateId < u`).
- a diff with `u <= last_u` is old and dropped (`diffs_stale`)

If the check fails, it's a gap: the book is thrown away, an `R` row is written, and it
starts again from the top. The diff that showed the gap stays in the buffer, since the next
snapshot may well connect to it.

**Reconnects.** A new connection gets a new `conn_epoch`. I can't know what was missed while
disconnected, so the first message of the new epoch throws the book away (`R` row) and
clears the buffer, and the book starts again from a fresh snapshot plus the new
connection's diffs. Nothing from the old connection is mixed in. Reconnects back off
0.5 s, 1 s, 2 s… up to 30 s.

**depth5 while live.** The assignment calls depth5 a sanity check and refresh, and that's
what it's used for: each one is compared with the book and then written over its top 5
levels. The problem is timing. Before writing the sync code, I recorded 30 seconds of each
venue and looked at how depth5 lines up with the diffs:

- on spot BTCUSDT, depth5 arrived one message *before* the diff ending at its id 94% of the
  time (283 of 300);
- on USD-M, its id was strictly *inside* a diff's `[U, u]` range 282 times out of 287;
- it isn't even consistent across symbols. In one 4-symbol run, ETHUSDT's depth5 arrived
  just *after* its diff every time, while BTCUSDT's arrived before.

Comparing a depth5 with the book only makes sense at the same update id, so:

- if its id equals the book's, it's applied right away (a `P` row);
- if it's ahead, it's held and laid on the book right before the diff that covers its id.
  Its `P` row comes right before that diff's `D` row;
- if it's behind, it's ignored (`stale`).

Laying it on early is safe because a diff carries the final quantity, as of `u`, of every
level that changed anywhere in its range. The levels the diff touches get their value as of
`u` anyway, and the levels it doesn't touch didn't change between the depth5 and `u`, so the
depth5's values for them are already current.

Every difference is counted. A depth5 disagreeing with a level the book should know exactly
(`levels_mismatched`) would mean a bug in the logic above. It has been **0 in every run**.

**Trades** don't change the book. The traded quantity also disappears from the book in the
next diff, so applying trades as well would remove that liquidity twice. A `T` row only
records the book at the time of the trade.

## Design notes

**One thread.** One `io_context` runs everything (socket reads, JSON parsing, book updates,
CSV formatting and the REST client) for all connections. Nothing is shared between threads,
so there are no locks, atomics or data races to reason about. In my runs BTCUSDT spot
averaged between 25 messages per second (a quiet Sunday morning) and 270 (a busy
afternoon, with bursts above 700), and a message takes a few microseconds to process (a
median of 2-16 µs depending on the mix), so the thread is idle almost all the time.
A separate writer thread would add a queue hop and cross-core traffic without taking
anything slow off this thread. For hundreds of symbols, the next step would be one
`io_context` per core with the shards spread across them. They already share nothing, so
that's still lock-free.

**Writing files.** Each file has a 256 KB buffer. Rows are formatted into it with
`std::to_chars` (no locale, no allocation) and written with one `fwrite` when it fills up,
plus once a second from a timer, so a crash loses at most about a second. The cost is that
the write happens on the network thread. In a quick test on my machine, a 256 KB write into
the page cache usually took about 80 µs and occasionally up to a millisecond. That's most
likely where the worst latencies below come from. If that mattered, or the storage were
slow, I'd hand full buffers to a writer thread through a single-producer/single-consumer
queue.

**Back-pressure and limits.** If processing ever fell behind, data would wait in the kernel's
socket buffer and TCP flow control would push back on Binance. Binance eventually drops slow
clients, so that would show up as a counted reconnect, not as growing memory. Nothing grows
without a limit: a websocket message or REST answer is at most 4 MB, at most 256 diffs are
buffered, a book side is capped at 5000 levels (once it reaches 6000, the 1000 furthest from
the top are dropped in one go), output buffers are flushed at a fixed size, and the stream
URL is at most 8 KB.

**The order book.** Each side is a `std::vector<Level>` sorted from worst to best, so the best
price is at the back ([src/order_book.hpp](src/order_book.hpp)). Almost all updates happen
near the top, which is near the back of the vector, so inserting or erasing a level only
shifts a few elements. Finding the level is a binary search, and the top 5 are just the last
5 elements, which are O(1) to read. So a diff with k levels costs O(k log n). Compared with a
`std::map`, there's no heap allocation per new level, no pointer chasing, and a whole side is
one contiguous block (5000 levels × 16 bytes = 80 KB). The worst case is an insert far from
the top, which shifts the whole vector, an 80 KB memmove of a few microseconds. The level
cap keeps that bounded. I also thought about an array indexed by price tick around the mid
(O(1) updates), but it needs each symbol's tick size and re-centring when the price moves,
which isn't worth it at this message rate.

**JSON.** I used simdjson's On-Demand API, and each frame is parsed once: read `"stream"`,
grab the raw bytes of `"data"` with `raw_json()`, then rewind that same object and decode
its fields. Those raw bytes are `payload_json`, which is why the CSV holds exactly what
Binance sent, with numbers and strings untouched. simdjson needs 64 readable bytes after its
input, and Beast's buffer doesn't promise that, so each frame is copied once into a padded
buffer (a memcpy of a few hundred bytes to a few KB).

**Allocations.** Once warmed up, processing a message does no heap allocation. Everything is
reused: the frame buffers, the preallocated simdjson parser, the decoded diff's vectors, the
book vectors (reserved up front), the CSV buffers and the diff ring buffer. A unit test
replaces `operator new` with a counter, warms up with about 2,200 mixed messages, sends
another 2,200 through the router, and checks that the count didn't move.

**Shutdown and ownership.** On Ctrl+C, SIGTERM or the end of `--duration`:

1. Timers and REST requests are cancelled.
2. Each connection sends a websocket close frame and keeps reading until Binance answers
   (Beast requires that), with a 2 s limit.
3. `io_context::run()` returns, and the files are flushed and closed by their owners
   (`unique_ptr<FILE>` with an `fclose` deleter).

Every async handler holds a `shared_ptr` to its connection (`shared_from_this()`), so a
connection can't be destroyed while an operation on it is pending. The pipelines reach the
REST client through a `weak_ptr`. A second Ctrl+C exits immediately, still flushing the
files. The ASan + UBSan build runs the tests and a live capture without any reports.

**Security.** No API keys or credentials; it only reads public market data. TLS checks the
certificate chain against the system CA store, sends SNI and verifies the hostname, for both
the websocket and the REST connections. `--ws-url` only accepts `wss://`.

## Replay

```bash
./build/binance_capture --replay samples/market_data_spot_BTCUSDT_2026-10-11.csv --output-dir ./replay
cmp samples/market_data_spot_BTCUSDT_2026-10-11_orderbook.csv replay/market_data_spot_BTCUSDT_2026-10-11_orderbook.csv
```

Replay makes no network calls. It reads the market data file and, if it's there, the
`_snapshots.csv` next to it. Each REST snapshot was saved together with the
`conn_epoch`/`conn_seq` of the market data row it arrived after (`after_conn_seq`), so replay
puts it back in exactly the same place. Everything then goes through the same `process()`
code as the live run. Every decision the book makes depends only on what's in those two
files, which is why the output is byte-for-byte identical.

Snapshots file columns:
`recv_tsec,recv_tnsec,venue,shard_id,conn_epoch,after_conn_seq,symbol,depth_limit,payload_json`.
It's a separate file because a REST snapshot isn't a websocket message, so it doesn't belong
in the market data file, whose `stream_kind` is fixed to the three stream types.

`--snapshot depth5` with `--replay` replays the same capture as if the book had been started
from depth5 messages, which is how I got the comparison numbers in this README.

[scripts/check_run.sh](scripts/check_run.sh) checks one capture in one go: the headers, 26
columns on every order book row, and the replay comparison.

```
$ scripts/check_run.sh samples/market_data_spot_BTCUSDT_2026-10-11.csv
OK    market data header
OK    order book header
OK    every order book row has 26 columns
OK    snapshots header
OK    replay gives the same market_data_spot_BTCUSDT_2026-10-11_orderbook.csv
```

## How I tested it

**Unit tests.** 30 of them, in [tests/unit_tests.cpp](tests/unit_tests.cpp) with a small
homemade `TEST`/`CHECK` setup:

- decimal parsing edge cases;
- RFC 4180 round trips;
- the book side;
- every sync case on both venues: seeding from REST and from depth5, buffered diffs older or
  newer than the snapshot, a snapshot that's too old, a hole in the buffer, gaps,
  reconnects, a held depth5 and a depth5 catching a wrong level. Each one checks the exact
  rows the event produces, e.g. `S D` for a REST seed with one buffered diff on top;
- both CSV formats checked byte for byte, including a held depth5's `P` row;
- replay with and without REST snapshots, plus the row counts (`D` rows = diffs applied,
  `P` rows = depth5s applied);
- the allocation counter;
- CLI parsing.

**Live runs.** Each one was followed by `scripts/check_run.sh`. All on my laptop (WSL2,
Ubuntu 22.04, i5-12500H, Release build, GCC 12), on 11 Oct 2026 with the code as it is now:

| Run | Messages | Gaps | Reconnects | REST snapshots | depth5 checks | Mismatched | Learned | Replay |
|---|---|---|---|---|---|---|---|---|
| spot BTCUSDT, 2 min ([the sample](#sample-run)) | 2,841 | 0 | 0 | 1 | 1,227 | 0 | 0 | identical |
| spot BTCUSDT, 5 min (the checklist command, `--duration 300`) | 7,664 | 0 | 0 | 1 | 3,083 | 0 | 0 | identical |
| USD-M BTCUSDT, 30 s | 628 | 0 | 0 | 1 | 229 | 0 | 0 | identical |
| spot BTC, ETH, BNB, SOL on 2 connections, 30 s | 2,120 | 0 | 0 | 4 | 918 | 0 | 0 | identical, all 4 |
| spot BTCUSDT, frozen for 150 s to force a reconnect | 2,529 | 0 | 1 | 2 | 895 | 0 | 0 | identical |
| spot BTC + ETH on 2 connections, ASan + UBSan build, 20 s | 1,084 | 0 | 0 | 2 | 385 | 0 | 0 | identical |
| spot BTCUSDT with `--snapshot depth5`, 20 s | 470 | 0 | 0 | - | 198 | 0 | 0 | identical |

In every run the order book file had exactly one `D` row per applied diff and one `P` row
per applied depth5 (rows counted against the counters), and its timestamps never went
backwards. A run pointed at a port where nothing listens exits with code 4 and
`connects=0 connect_failures=4`.

I ran the same set on 8 Oct, while the market was much busier: 31,867 messages in the
2-minute run, 38,859 in the 5-minute one. The book results were the same: no gaps, no
mismatches, every replay identical.

The fresh-clone build was also checked both ways the assignment's checklist does it: Ninja
with `g++-12` and `-Wall -Wextra -O2`, and plain `cmake -B build && cmake --build build`
(GCC 11.4, make). Both gave no warnings and all tests passed.

The forced reconnect: I froze the process with `SIGSTOP` so Binance would drop the
connection. On 8 Oct 75 s was enough; on this quiet morning Binance kept a connection that
had been frozen for 78 s, so this run froze it for 150 s. When it resumed, the read failed
("End of file"), it reconnected 0.5 s later with `conn_epoch=1`, wrote an `R` row, had a
new REST snapshot about 0.6 s after that and carried on.

## Performance

All from the runs above, on the same laptop.

- **Per message.** This is the time from a websocket message being fully read to all its
  CSV rows being formatted, measured with `steady_clock` into power-of-two buckets. The
  median was 8-16 µs this morning, when most messages were diffs and depth5s. On 8 Oct,
  when most messages were trades (cheap: no book change), it was 2-4 µs. p99 was under
  131 µs on spot and under 262 µs on USD-M, whose diffs often carry hundreds of levels. The
  slowest single message in any run took 2.1 ms. The tail includes the occasional inline
  256 KB `fwrite` (see Writing files above).
- **Load.** The 5-minute checklist run averaged 25 messages per second and used 0.86 s of
  CPU in total (about 0.3% of one core) and 15 MB of memory. On 8 Oct the same command
  averaged 130 messages per second, and that day's 2-minute run had bursts above 700.
- **Replay** runs the same processing code without the network, so it shows how fast that
  code is on its own. The 5-minute capture (7,664 messages, 3 MB) replays in 23 ms, about
  330,000 messages per second including reading the CSV back in. 8 Oct's trade-heavy
  5-minute capture went at about 400,000 per second.

## Sample run

[samples/](samples) has 2 minutes of BTCUSDT spot, recorded 11 Oct 2026 between 02:16 and
02:18 UTC (a quiet Sunday morning) with:

```bash
./build/binance_capture --venue spot --symbols BTCUSDT --output-dir ./output --duration 120
```

| File | Rows | Size |
|---|---|---|
| [market_data_spot_BTCUSDT_2026-10-11.csv](samples/market_data_spot_BTCUSDT_2026-10-11.csv) | 2,841 (1,232 depth_diff, 1,232 depth5, 377 trade) | 1.2 MB |
| [market_data_spot_BTCUSDT_2026-10-11_orderbook.csv](samples/market_data_spot_BTCUSDT_2026-10-11_orderbook.csv) | 2,833 (1 `S`, 1,229 `D`, 1,227 `P`, 376 `T`) | 0.7 MB |
| [market_data_spot_BTCUSDT_2026-10-11_snapshots.csv](samples/market_data_spot_BTCUSDT_2026-10-11_snapshots.csv) | 1 (5000 levels per side) | 0.4 MB |

These are the counters printed at the end of the run:

```
[live] transport: connects=1 connect_failures=0 disconnects=0 messages=2841 bytes=1006191 envelope_errors=0 unknown_streams=0 minified_payloads=0
[live] book: diffs_applied=1229 diffs_stale=3 diffs_buffered=5 buffer_overflows=0 gaps=0 epoch_resets=0 seeds=1 seed_failures=0 decode_errors=0
[live] depth5: checks=1227 stale=0 superseded=0 ignored=5 levels_mismatched=0 levels_learned=0 | trades=377 uncertain_rows=0
[live] rest: requested=1 received=1 unused=0 errors=0 weight_1m=750
[live] output: market_rows=2841 book_rows=2833 write_errors=0
[live] latency (read->rows, ns): n=2841 mean=18068 p50<=16384 p99<=65536 max=2109788
```

- The REST snapshot was requested when the first diff arrived and came back 408 ms later.
  In the meantime 5 diffs were buffered: 3 were already covered by the snapshot
  (`diffs_stale=3`) and 2 were applied on top. The 5 depth5 messages from that window were
  ignored. (`weight_1m=750` because I had two other captures running at the same time.)
- There were no gaps. Every depth5 after that (1,227 of them) was applied and checked
  against the book; none disagreed, and none showed a level the book didn't already have.
- The row counts line up: 1,229 `D` rows for `diffs_applied=1229` and 1,227 `P` rows for
  `checks=1227`. One trade arrived before the book was live, so it has a market data row but
  no `T` row (377 trades, 376 `T` rows).
- Replaying the same capture with `--snapshot depth5` gives `levels_learned=2`: twice,
  depth5 showed the book a level it didn't know about. On the much busier 8 Oct sample the
  same comparison gave 35.

The first rows of each file (payloads cut short):

```
recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json
1791685015,430905261,spot,trade,0,0,0,BTCUSDT,"{""e"":""trade"",""E"":1791685015342,""s"":""BTCUSDT"",""t"":6752527017,""p"":""83053.45000000"",...
1791685015,490731037,spot,depth5,0,0,1,BTCUSDT,"{""lastUpdateId"":101273591789,""bids"":[[""83053.44000000"",""4.40461000""],...
1791685015,491011445,spot,depth_diff,0,0,2,BTCUSDT,"{""e"":""depthUpdate"",""E"":1791685015415,""s"":""BTCUSDT"",""U"":101273591765,""u"":101273591789,...
```

```
tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4
1791685015,899220099,1,2014473836,S,N,8305344000000,8305343000000,8305342000000,8305341000000,8305340000000,434467000,83000,13000,14000,5000000,8305345000000,8305346000000,8305347000000,8305365000000,8305377000000,220485000,38069000,13000,194000,363000
1791685015,899220099,2,2014473836,D,N,8305344000000,8305343000000,8305342000000,8305341000000,8305340000000,436274000,83000,13000,14000,5000000,8305345000000,8305346000000,8305347000000,8305365000000,8305377000000,220685000,38069000,13000,194000,363000
1791685015,899220099,3,2014473836,D,N,8305344000000,8305343000000,8305342000000,8305341000000,8305340000000,436346000,83000,13000,14000,5000000,8305345000000,8305346000000,8305347000000,8305365000000,8305400000000,220692000,38076000,13000,194000,14000
1791685015,990514752,4,2014473836,P,N,8305344000000,8305343000000,8305342000000,8305341000000,8305340000000,438153000,83000,13000,14000,5000000,8305345000000,8305346000000,8305347000000,8305365000000,8305400000000,216492000,38076000,13000,194000,14000
1791685015,990514752,5,2014473836,D,N,8305344000000,8305343000000,8305342000000,8305341000000,8305340000000,438153000,83000,13000,14000,5000000,8305345000000,8305346000000,8305347000000,8305365000000,8305400000000,216492000,38076000,13000,194000,14000
```

- The REST snapshot (`lastUpdateId` 101273591826) arrived right after market data row
  `conn_seq=10`. The buffered diffs ending at ...789, ...805 and ...826 were already in it.
  The next two (...827-...831 and ...832-...848) connect to it and were applied on top. So
  the book starts with an `S` row (the snapshot alone) and two `D` rows, all three stamped
  with the time the REST answer arrived.
- Rows 4 and 5 are the usual spot pattern. The depth5 with id ...853 (`conn_seq=11`) arrived
  15 µs before the diff ending at ...853 (`conn_seq=12`). It was held, then applied right
  before that diff. Its `P` row (the top 5 as of ...853) and the diff's `D` row both carry
  the diff's receive time.
- `8305344000000` = 83053.44000000 USDT.

## Things that went wrong along the way

- **Half a message glued onto the next connection.** That reconnect test, the first time I
  ran it, produced a garbage first frame after reconnecting:
  `{"stream":"btcusdt@trade","data":{"e{"stream":...`. A read that dies halfway through a
  message leaves its partial bytes in Beast's buffer, and I was reusing the buffer for the
  new connection. The parser rejected the frame, so no wrong data got through, but a message
  was lost. Now the buffer is emptied at the start of every connection.
- **Shutdown took about 11 seconds.** I stopped reading after sending the close frame, but
  Beast needs you to keep reading until the close comes back. Binance is also sometimes slow
  to answer, and Beast's close timeout was the 10 s handshake timeout. Now it keeps reading,
  with its own 2 s limit.
- **Ctrl+C could hang forever.** I found this one by reading the code, not by hitting it. The
  signal handler re-armed its wait *after* starting the shutdown, so if every connection
  stopped immediately, the new wait kept `io_context` alive. It re-arms first now.
- **simdjson was running its scalar fallback**, see the `-march` note under
  [Building](#building).
- **The book started from depth5 at first.** That worked, but the counters showed depth5
  regularly telling the book about levels it had never seen (`levels_learned` > 0), which is
  what moved me to the REST snapshot.
- **Not every applied event had its own row.** A depth5 held for its diff was applied, but
  it only showed up inside that diff's `D` row, so a busy capture could have zero `P` rows.
  The diffs applied on top of a snapshot were folded into its `S` row. The book itself was
  right, but the assignment asks for a row after every applied depthUpdate and depth5, and
  `grep -c ',P,'` printing 0 says otherwise. Someone reviewing the code pointed it out. Now
  `BookSync` reports every change through a callback and the pipeline writes a row for each,
  and the tests check the exact row sequence per event. Replaying the old sample with the fix
  gave the same rows as before plus the missing `P` and `D` rows. The only other difference
  is that the `S` row now shows the snapshot on its own.
- **The provided `.gitignore` has `*.csv`**, which would have left the sample run out of the
  repo. I added `!samples/*.csv`.

## What I'd do next

- Move the `fwrite` calls to a writer thread, handing it full buffers through a small SPSC
  queue of preallocated buffers, then measure the tail latency again. It's the one thing on
  the network thread that can take a millisecond.
- Rotate the output files at UTC midnight. Right now the name uses the date the run started.
- Take receive timestamps from the kernel (`SO_TIMESTAMPING`) instead of reading the clock in
  the handler.
- Run a longer soak test, several hours with a few forced reconnects.
- For hundreds of symbols: one `io_context` per core, with the shards spread across them.

## Repo layout

```
src/
  main.cpp               parse arguments, run live or replay, print counters
  config.*               command line, limits, stream URLs
  live.*                 websocket connections (Beast), reconnects, timers, shutdown
  snapshot_fetcher.*     REST depth snapshots
  router.*               raw frame -> symbol, slices out payload_json
  pipeline.*             one symbol: market row -> decode -> book -> order book row
  decoder.*              JSON -> DepthDiff / Depth5 / RestSnapshot / Trade
  book_sync.*            update ids, gaps, reconnects, depth5 checks
  order_book.*           the book (two sorted vectors)
  decimal.hpp            "62000.10" -> 6200010000000
  csv.*                  RFC 4180 writer and reader
  replay.*               replay mode
  metrics.*, log.*, net.hpp, types.hpp
tests/                   unit tests and an operator new counter
scripts/check_run.sh     header, column and replay check for one capture
samples/                 the 2-minute sample run
```
