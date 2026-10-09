// unit tests - no framework, just a couple of macros. run: ./build/tests/unit_tests

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "book_sync.hpp"
#include "config.hpp"
#include "csv.hpp"
#include "decimal.hpp"
#include "order_book.hpp"
#include "pipeline.hpp"
#include "replay.hpp"
#include "router.hpp"

using namespace capture;
namespace fs = std::filesystem;

// number of global operator new calls so far (alloc_counter.cpp)
size_t allocationCount();

// ---- tiny test framework

struct TestCase {
    const char* name;
    void (*fn)();
};
static std::vector<TestCase>& allTests() {
    static std::vector<TestCase> tests;
    return tests;
}
struct AddTest {
    AddTest(const char* name, void (*fn)()) { allTests().push_back({name, fn}); }
};
static int failures = 0;

static void fail(const char* file, int line, const std::string& what) {
    failures++;
    std::fprintf(stderr, "    %s:%d: %s\n", file, line, what.c_str());
}

#define TEST(name)                                  \
    static void name();                             \
    static const AddTest add_##name(#name, name);   \
    static void name()

#define CHECK(cond)                                         \
    do {                                                    \
        if (!(cond)) fail(__FILE__, __LINE__, #cond);       \
    } while (0)

#define CHECK_EQ(a, b)                                                              \
    do {                                                                            \
        const auto& a_ = (a);                                                       \
        const auto& b_ = (b);                                                       \
        if (!(a_ == b_)) {                                                          \
            std::ostringstream os;                                                  \
            os << #a " == " #b " but got [" << a_ << "] vs [" << b_ << "]";         \
            fail(__FILE__, __LINE__, os.str());                                     \
        }                                                                           \
    } while (0)

// ---- helpers

static fs::path freshDir(const std::string& name) {
    fs::path dir = fs::temp_directory_path() / ("capture_tests_" + name);
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

static std::string readFile(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

static std::vector<std::string> readLines(const fs::path& path) {
    std::vector<std::string> out;
    std::istringstream in(readFile(path));
    for (std::string line; std::getline(in, line);) out.push_back(line);
    return out;
}

// naive split, fine for the columns we look at
static std::string col(const std::string& row, int index) {
    size_t start = 0;
    for (int i = 0; i < index; i++) start = row.find(',', start) + 1;
    return row.substr(start, row.find(',', start) - start);
}

static int countCommas(const std::string& s) {
    int n = 0;
    for (char c : s) n += c == ',';
    return n;
}

static DepthDiff makeDiff(uint64_t first, uint64_t last, std::vector<Level> bids, std::vector<Level> asks,
                          uint64_t prev = 0) {
    DepthDiff d;
    d.firstId = first;
    d.lastId = last;
    d.prevLastId = prev;
    d.bids = std::move(bids);
    d.asks = std::move(asks);
    return d;
}

static Depth5 makeDepth5(uint64_t id, const std::vector<Level>& bids, const std::vector<Level>& asks) {
    Depth5 s;
    s.lastId = id;
    for (const Level& l : bids) s.bids[static_cast<size_t>(s.nBids++)] = l;
    for (const Level& l : asks) s.asks[static_cast<size_t>(s.nAsks++)] = l;
    return s;
}

// five bids going down from top / five asks going up from bottom, qty 1 each
static std::vector<Level> bidsFrom(int64_t top) { return {{top, 1}, {top - 1, 1}, {top - 2, 1}, {top - 3, 1}, {top - 4, 1}}; }
static std::vector<Level> asksFrom(int64_t bottom) { return {{bottom, 1}, {bottom + 1, 1}, {bottom + 2, 1}, {bottom + 3, 1}, {bottom + 4, 1}}; }

using JsonLevels = std::vector<std::pair<std::string, std::string>>;

static std::string jsonLevels(const JsonLevels& levels) {
    std::string s = "[";
    for (size_t i = 0; i < levels.size(); i++) {
        if (i) s += ',';
        s += "[\"" + levels[i].first + "\",\"" + levels[i].second + "\"]";
    }
    return s + "]";
}

static std::string diffFrame(uint64_t first, uint64_t last, const JsonLevels& bids, const JsonLevels& asks) {
    return R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1700000000000,"s":"BTCUSDT","U":)" +
           std::to_string(first) + ",\"u\":" + std::to_string(last) + ",\"b\":" + jsonLevels(bids) +
           ",\"a\":" + jsonLevels(asks) + "}}";
}

static std::string depth5Frame(uint64_t id, const JsonLevels& bids, const JsonLevels& asks) {
    return R"({"stream":"btcusdt@depth5@100ms","data":{"lastUpdateId":)" + std::to_string(id) +
           ",\"bids\":" + jsonLevels(bids) + ",\"asks\":" + jsonLevels(asks) + "}}";
}

static std::string tradeFrame(const std::string& price, const std::string& qty, bool buyerIsMaker) {
    return R"({"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":")" + price +
           "\",\"q\":\"" + qty + "\",\"T\":1,\"m\":" + (buyerIsMaker ? "true" : "false") + ",\"M\":true}}";
}

static const JsonLevels BIDS5 = {{"100.00", "1"}, {"99.00", "1"}, {"98.00", "1"}, {"97.00", "1"}, {"96.00", "1"}};
static const JsonLevels ASKS5 = {{"101.00", "1"}, {"102.00", "1"}, {"103.00", "1"}, {"104.00", "1"}, {"105.00", "1"}};
static constexpr int64_t T0 = 1'700'000'000'123'456'789;

// ---- decimal parsing

static_assert([] {
    int64_t v = 0;
    return parseScaled("1.5", v) == DecimalError::None && v == 150'000'000;
}());

TEST(decimal_parses_exactly) {
    int64_t v = -1;
    CHECK(parseScaled("62000.10", v) == DecimalError::None);
    CHECK_EQ(v, 6200010000000);
    CHECK(parseScaled("62000.10000000", v) == DecimalError::None);
    CHECK_EQ(v, 6200010000000);
    CHECK(parseScaled("0.00000001", v) == DecimalError::None);
    CHECK_EQ(v, 1);
    CHECK(parseScaled("0", v) == DecimalError::None);
    CHECK_EQ(v, 0);
    CHECK(parseScaled("5", v) == DecimalError::None);
    CHECK_EQ(v, 500'000'000);
    CHECK(parseScaled("1.123456780", v) == DecimalError::None);  // 9th digit is 0, still exact
    CHECK_EQ(v, 112'345'678);
    CHECK(parseScaled("92233720368.54775807", v) == DecimalError::None);  // exactly INT64_MAX
    CHECK_EQ(v, INT64_MAX);
}

TEST(decimal_rejects_what_it_cant_represent) {
    int64_t v = 0;
    CHECK(parseScaled("", v) == DecimalError::Empty);
    CHECK(parseScaled(".", v) == DecimalError::BadChar);
    CHECK(parseScaled("-1", v) == DecimalError::BadChar);
    CHECK(parseScaled("1e5", v) == DecimalError::BadChar);
    CHECK(parseScaled("1.2.3", v) == DecimalError::BadChar);
    CHECK(parseScaled("1.123456789", v) == DecimalError::TooPrecise);
    CHECK(parseScaled("92233720368.54775808", v) == DecimalError::Overflow);
    CHECK(parseScaled("100000000000", v) == DecimalError::Overflow);
}

// ---- csv

TEST(csv_escaping_follows_rfc4180) {
    OutBuffer out;
    appendCsvField(out, "plain");
    out.push('|');
    appendCsvField(out, "a,b");
    out.push('|');
    appendCsvField(out, R"({"k":"v"})");
    out.push('|');
    appendCsvField(out, "line\nbreak");
    out.push('|');
    appendCsvField(out, "");
    CHECK_EQ(std::string(out.view()), std::string("plain|\"a,b\"|\"{\"\"k\"\":\"\"v\"\"}\"|\"line\nbreak\"|"));
}

TEST(csv_reader_reads_back_what_the_writer_wrote) {
    fs::path path = freshDir("csv") / "roundtrip.csv";
    std::vector<std::vector<std::string>> rows = {
        {"a", "b,c", "\"quoted\""}, {"", "multi\nline", R"({"k":[1,2],"s":"x,y"})"}, {"last"}};
    {
        CsvWriter w;
        CHECK(w.open(path.string(), "h1,h2,h3"));
        for (const auto& row : rows) {
            for (size_t i = 0; i < row.size(); i++) {
                if (i) w.row().push(',');
                appendCsvField(w.row(), row[i]);
            }
            w.endRow();
        }
    }
    CsvReader reader;
    CHECK(reader.open(path.string()));
    std::vector<std::string> fields;
    size_t n = 0;
    CHECK(reader.next(fields, n));
    CHECK_EQ(n, 3u);
    CHECK_EQ(fields[2], std::string("h3"));
    for (const auto& row : rows) {
        CHECK(reader.next(fields, n));
        CHECK_EQ(n, row.size());
        for (size_t i = 0; i < row.size() && i < n; i++) CHECK_EQ(fields[i], row[i]);
    }
    CHECK(!reader.next(fields, n));
    CHECK(reader.error().empty());
}

TEST(csv_reader_handles_crlf_and_reports_bad_quotes) {
    fs::path dir = freshDir("csv_crlf");
    std::ofstream(dir / "crlf.csv", std::ios::binary) << "a,b\r\n\"c\",d\r\n";
    std::ofstream(dir / "bad.csv", std::ios::binary) << "a,\"unterminated\n";

    CsvReader reader;
    std::vector<std::string> f;
    size_t n = 0;
    CHECK(reader.open((dir / "crlf.csv").string()));
    CHECK(reader.next(f, n) && n == 2 && f[1] == "b");
    CHECK(reader.next(f, n) && n == 2 && f[0] == "c" && f[1] == "d");
    CHECK(!reader.next(f, n) && reader.error().empty());

    CsvReader bad;
    CHECK(bad.open((dir / "bad.csv").string()));
    CHECK(!bad.next(f, n));
    CHECK(!bad.error().empty());
}

// ---- order book

TEST(book_keeps_best_first_and_zero_removes) {
    BookSide<Side::Bid> bids;
    bids.set(100, 5);
    bids.set(102, 1);
    bids.set(101, 2);
    CHECK_EQ(bids.size(), 3);
    CHECK_EQ(bids.best(0).price, 102);
    CHECK_EQ(bids.best(2).price, 100);
    bids.set(101, 7);  // qty is absolute, it replaces
    CHECK_EQ(bids.best(1).qty, 7);
    bids.set(102, 0);
    CHECK_EQ(bids.best(0).price, 101);
    bids.set(999, 0);  // removing a level we never had does nothing
    CHECK_EQ(bids.size(), 2);

    BookSide<Side::Ask> asks;
    asks.set(105, 1);
    asks.set(103, 1);
    asks.set(104, 1);
    CHECK_EQ(asks.best(0).price, 103);
    CHECK_EQ(asks.best(2).price, 105);
}

TEST(replace_top_tracks_verified_part) {
    BookSide<Side::Bid> bids;
    const Level seed[] = {{100, 1}, {99, 1}, {98, 1}, {97, 1}, {96, 1}};
    bids.replaceTop(seed, 5, 5, nullptr);
    CHECK(bids.slotVerified(4));
    CHECK(!bids.slotVerified(5));  // nothing known below 96

    // a diff adds a deep level and removes the two best
    bids.set(90, 3);
    bids.set(100, 0);
    bids.set(99, 0);
    CHECK(bids.slotVerified(2));   // 96 is still inside the snapshot range
    CHECK(!bids.slotVerified(3));  // 90 isn't - there could be levels between 96 and 90

    // next snapshot shows 95 and 94 (new to us = learned) and a different qty at 97 (a real mismatch)
    const Level snap[] = {{98, 1}, {97, 2}, {96, 1}, {95, 4}, {94, 4}};
    RefreshStats st = bids.replaceTop(snap, 5, 5, nullptr);
    CHECK_EQ(st.mismatched, 1);
    CHECK_EQ(st.learned, 2);
    CHECK_EQ(bids.best(1).qty, 2);
    CHECK_EQ(bids.best(3).price, 95);
    CHECK_EQ(bids.best(5).price, 90);  // deeper level survives
    CHECK(bids.slotVerified(4));
}

TEST(replace_top_skips_levels_the_next_diff_overwrites) {
    BookSide<Side::Ask> asks;
    const Level seed[] = {{10, 1}, {11, 1}, {12, 1}, {13, 1}, {14, 1}};
    asks.replaceTop(seed, 5, 5, nullptr);
    const Level snap[] = {{10, 5}, {11, 1}, {12, 1}, {13, 1}, {14, 1}};
    std::vector<Level> nextDiff = {{10, 5}};
    CHECK_EQ(asks.replaceTop(snap, 5, 5, &nextDiff).mismatched, 0);
    const Level snap2[] = {{10, 6}, {11, 1}, {12, 1}, {13, 1}, {14, 1}};
    CHECK_EQ(asks.replaceTop(snap2, 5, 5, nullptr).mismatched, 1);
}

TEST(short_snapshot_replaces_whole_side) {
    BookSide<Side::Ask> asks;
    asks.set(50, 1);  // stale level the snapshot doesn't have anymore
    const Level snap[] = {{10, 1}, {11, 1}};
    asks.replaceTop(snap, 2, 5, nullptr);
    CHECK_EQ(asks.size(), 2);
    CHECK(asks.slotVerified(4));  // side really only has 2 levels
}

TEST(book_trims_far_levels) {
    using Bids = BookSide<Side::Bid>;
    Bids bids;
    int total = Bids::MAX_LEVELS + Bids::TRIM_SLACK + 1;
    for (int i = 1; i <= total; i++) bids.set(i, 1);
    CHECK_EQ(bids.size(), Bids::MAX_LEVELS);
    CHECK_EQ(bids.best(0).price, total);
    CHECK_EQ(bids.trimmed(), static_cast<uint64_t>(total - Bids::MAX_LEVELS));
}

// ---- sync logic, spot

TEST(spot_seeds_from_depth5_and_detects_gaps) {
    Metrics m;
    BookSync s(Venue::Spot, m);
    s.onEpoch(0);
    CHECK(s.onDiff(makeDiff(90, 95, {{100, 9}}, {})) == BookEffect::None);  // not live yet, buffered
    CHECK(s.onDepth5(makeDepth5(100, bidsFrom(100), asksFrom(101))) == BookEffect::Applied);
    CHECK(s.live());
    CHECK_EQ(m.diffsStale, 1u);  // buffered diff was older than the snapshot
    CHECK_EQ(s.book().bids().best(0).qty, 1);
    CHECK(s.onDiff(makeDiff(96, 100, {}, {})) == BookEffect::None);  // already covered
    CHECK(s.onDiff(makeDiff(101, 103, {{100, 0}}, {})) == BookEffect::Applied);
    CHECK_EQ(s.book().bids().best(0).price, 99);
    CHECK(s.onDiff(makeDiff(110, 112, {}, {})) == BookEffect::Reset);  // 104..109 missing
    CHECK(!s.live());
    CHECK_EQ(m.gaps, 1u);
    CHECK_EQ(s.book().bids().size(), 0);
}

TEST(spot_replays_buffered_diffs_newer_than_seed) {
    Metrics m;
    BookSync s(Venue::Spot, m);
    s.onEpoch(0);
    s.onDiff(makeDiff(95, 100, {}, {}));
    s.onDiff(makeDiff(101, 104, {{100, 7}}, {}));
    CHECK(s.onDepth5(makeDepth5(100, bidsFrom(100), asksFrom(101))) == BookEffect::Applied);
    CHECK_EQ(s.lastUpdateId(), 104u);
    CHECK_EQ(s.book().bids().best(0).qty, 7);
}

TEST(spot_seed_waits_if_snapshot_too_old) {
    Metrics m;
    BookSync s(Venue::Spot, m);
    s.onEpoch(0);
    s.onDiff(makeDiff(105, 110, {{100, 3}}, {}));
    CHECK(s.onDepth5(makeDepth5(100, bidsFrom(100), asksFrom(101))) == BookEffect::None);  // 101..104 missing
    CHECK(!s.live());
    CHECK_EQ(m.seedFailures, 1u);
    CHECK(s.onDepth5(makeDepth5(107, bidsFrom(100), asksFrom(101))) == BookEffect::Applied);
    CHECK_EQ(s.lastUpdateId(), 110u);
    CHECK_EQ(s.book().bids().best(0).qty, 3);
}

TEST(depth5_ahead_of_its_diff_gets_applied_with_it) {
    Metrics m;
    BookSync s(Venue::Spot, m);
    s.onEpoch(0);
    s.onDepth5(makeDepth5(100, bidsFrom(100), asksFrom(101)));
    // this is the usual order on spot: depth5(L) shows up just before the diff ending at L
    CHECK(s.onDepth5(makeDepth5(103, {{100, 2}, {99, 1}, {98, 1}, {97, 1}, {96, 1}}, asksFrom(101))) == BookEffect::None);
    CHECK(s.onDiff(makeDiff(101, 103, {{100, 2}}, {})) == BookEffect::Applied);
    CHECK_EQ(m.depth5Checks, 1u);
    CHECK_EQ(m.levelsMismatched, 0u);
    CHECK_EQ(s.book().bids().best(0).qty, 2);
}

TEST(depth5_catches_and_fixes_a_wrong_level) {
    Metrics m;
    BookSync s(Venue::Spot, m);
    s.onEpoch(0);
    s.onDepth5(makeDepth5(100, bidsFrom(100), asksFrom(101)));
    s.onDiff(makeDiff(101, 101, {{100, 5}}, {}));
    CHECK(s.onDepth5(makeDepth5(101, {{100, 4}, {99, 1}, {98, 1}, {97, 1}, {96, 1}}, asksFrom(101))) == BookEffect::Applied);
    CHECK_EQ(m.levelsMismatched, 1u);
    CHECK_EQ(s.book().bids().best(0).qty, 4);
    CHECK(s.onDepth5(makeDepth5(90, bidsFrom(100), asksFrom(101))) == BookEffect::None);
    CHECK_EQ(m.depth5Stale, 1u);
}

TEST(new_epoch_resets_the_book) {
    Metrics m;
    BookSync s(Venue::Spot, m);
    CHECK(s.onEpoch(0) == BookEffect::None);
    s.onDepth5(makeDepth5(100, bidsFrom(100), asksFrom(101)));
    CHECK(s.onEpoch(0) == BookEffect::None);
    CHECK(s.onEpoch(1) == BookEffect::Reset);
    CHECK(!s.live());
    CHECK_EQ(s.book().asks().size(), 0);
    CHECK_EQ(m.epochResets, 1u);
}

// ---- sync logic, futures

TEST(usdm_chains_on_pu) {
    Metrics m;
    BookSync s(Venue::UsdM, m);
    s.onEpoch(0);
    // on futures the depth5 id falls inside a diff's range, here (1000, 1010]
    CHECK(s.onDepth5(makeDepth5(1005, bidsFrom(100), asksFrom(101))) == BookEffect::Applied);
    CHECK(s.onDiff(makeDiff(1001, 1010, {{100, 3}}, {}, 1000)) == BookEffect::Applied);
    CHECK(s.onDiff(makeDiff(1011, 1020, {}, {{101, 0}}, 1010)) == BookEffect::Applied);
    CHECK_EQ(s.book().bids().best(0).qty, 3);
    CHECK_EQ(s.book().asks().best(0).price, 102);
    CHECK(s.onDiff(makeDiff(1031, 1040, {}, {}, 1025)) == BookEffect::Reset);  // pu != last u
    CHECK_EQ(m.gaps, 1u);
}

TEST(usdm_depth5_inside_next_diff_is_checked_against_it) {
    Metrics m;
    BookSync s(Venue::UsdM, m);
    s.onEpoch(0);
    s.onDepth5(makeDepth5(1005, bidsFrom(100), asksFrom(101)));
    s.onDiff(makeDiff(1001, 1010, {}, {}, 1000));
    // snapshot at 1015 already shows bid 100 = 8, which the next diff (1011..1020) sets - not a mismatch
    CHECK(s.onDepth5(makeDepth5(1015, {{100, 8}, {99, 1}, {98, 1}, {97, 1}, {96, 1}}, asksFrom(101))) == BookEffect::None);
    CHECK(s.onDiff(makeDiff(1011, 1020, {{100, 8}}, {}, 1010)) == BookEffect::Applied);
    CHECK_EQ(m.depth5Checks, 1u);
    CHECK_EQ(m.levelsMismatched, 0u);
}

// ---- end to end through the router

TEST(router_writes_both_csvs_exactly) {
    fs::path dir = freshDir("router");
    Metrics m;
    SymbolPipeline p(Venue::Spot, "BTCUSDT", m);
    CHECK(p.open((dir / "md.csv").string(), (dir / "md_orderbook.csv").string()));
    Router r(Venue::Spot, m);
    r.add(p);
    r.onMessage(0, 0, 0, T0, depth5Frame(100, BIDS5, ASKS5));
    r.onMessage(0, 0, 1, T0 + 1, diffFrame(101, 101, {{"100.50", "0.5"}}, {}));
    r.onMessage(0, 0, 2, T0 + 2, tradeFrame("100.50", "0.1", true));
    r.onMessage(0, 0, 3, T0 + 3, R"({"stream":"ethusdt@trade","data":{}})");
    r.onMessage(0, 0, 4, T0 + 4, "not json");
    p.close();

    auto md = readLines(dir / "md.csv");
    CHECK_EQ(md.size(), 4u);
    CHECK_EQ(md[0], std::string(MARKET_HEADER));
    CHECK_EQ(md[1], std::string(R"(1700000000,123456789,spot,depth5,0,0,0,BTCUSDT,"{""lastUpdateId"":100,""bids"":[[""100.00"",""1""],[""99.00"",""1""],[""98.00"",""1""],[""97.00"",""1""],[""96.00"",""1""]],""asks"":[[""101.00"",""1""],[""102.00"",""1""],[""103.00"",""1""],[""104.00"",""1""],[""105.00"",""1""]]}")"));
    CHECK_EQ(col(md[2], 3), std::string("depth_diff"));
    CHECK_EQ(col(md[3], 6), std::string("2"));

    auto ob = readLines(dir / "md_orderbook.csv");
    CHECK_EQ(ob.size(), 4u);
    CHECK_EQ(ob[0], std::string(BOOK_HEADER));
    for (const auto& line : ob) CHECK_EQ(countCommas(line), 25);
    CHECK_EQ(ob[1], std::string("1700000000,123456789,1,") + std::to_string(instrumentId(Venue::Spot, "BTCUSDT")) +
                        ",P,N,10000000000,9900000000,9800000000,9700000000,9600000000,"
                        "100000000,100000000,100000000,100000000,100000000,"
                        "10100000000,10200000000,10300000000,10400000000,10500000000,"
                        "100000000,100000000,100000000,100000000,100000000");
    CHECK_EQ(col(ob[2], 4), std::string("D"));
    CHECK_EQ(col(ob[2], 5), std::string("B"));
    CHECK_EQ(col(ob[2], 6), std::string("10050000000"));  // new best bid 100.50
    CHECK_EQ(col(ob[2], 11), std::string("50000000"));    // size 0.5
    CHECK_EQ(col(ob[3], 4), std::string("T"));
    CHECK_EQ(col(ob[3], 5), std::string("B"));  // buyer was maker -> bid got hit
    CHECK_EQ(m.unknownStreams, 1u);
    CHECK_EQ(m.envelopeErrors, 1u);
}

TEST(router_minifies_payload_with_spaces) {
    fs::path dir = freshDir("minify");
    Metrics m;
    SymbolPipeline p(Venue::Spot, "BTCUSDT", m);
    CHECK(p.open((dir / "md.csv").string(), (dir / "ob.csv").string()));
    Router r(Venue::Spot, m);
    r.add(p);
    // keys in the other order + whitespace everywhere
    r.onMessage(0, 0, 0, T0, R"({"data": { "p" : "1.5", "q":"2", "m": false } , "stream":"btcusdt@trade"})");
    p.close();
    auto md = readLines(dir / "md.csv");
    CHECK_EQ(md.size(), 2u);
    CHECK_EQ(col(md[1], 8), std::string(R"("{""p"":""1.5"")"));
    CHECK_EQ(m.minifiedPayloads, 1u);
    CHECK_EQ(m.decodeErrors, 0u);
}

TEST(replay_matches_live_byte_for_byte) {
    fs::path dir = freshDir("replay");
    fs::path market = dir / "market_data_spot_BTCUSDT_2026-01-01.csv";
    fs::path book = dir / "market_data_spot_BTCUSDT_2026-01-01_orderbook.csv";
    Metrics live;
    {
        SymbolPipeline p(Venue::Spot, "BTCUSDT", live);
        CHECK(p.open(market.string(), book.string()));
        Router r(Venue::Spot, live);
        r.add(p);
        uint64_t seq = 0;
        int64_t t = T0;
        auto feed = [&](uint64_t epoch, const std::string& frame) { r.onMessage(0, epoch, seq++, t += 1000, frame); };
        feed(0, diffFrame(90, 99, {{"99.00", "2"}}, {}));     // before any snapshot -> buffered
        feed(0, depth5Frame(100, BIDS5, ASKS5));              // seed
        feed(0, depth5Frame(102, BIDS5, ASKS5));              // ahead -> held
        feed(0, diffFrame(101, 102, {}, {{"101.00", "0"}}));  // held depth5 applied, then this diff
        feed(0, tradeFrame("101.50", "0.2", false));
        feed(0, diffFrame(103, 103, {{"100.10", "1.25"}}, {{"101.10", "3"}}));
        feed(0, diffFrame(200, 201, {}, {}));                 // gap -> R row
        feed(0, depth5Frame(201, BIDS5, ASKS5));              // reseed
        feed(1, diffFrame(300, 301, {}, {}));                 // reconnect -> R row
        feed(1, depth5Frame(301, BIDS5, ASKS5));
        feed(1, diffFrame(302, 302, {{"99.99", "0.00000001"}}, {}));
        p.close();
    }
    Metrics replayed;
    CHECK_EQ(runReplay(market.string(), (dir / "replay").string(), replayed), 0);
    std::string liveBook = readFile(book);
    std::string replayBook = readFile(dir / "replay" / book.filename());
    CHECK(!liveBook.empty());
    CHECK(liveBook == replayBook);
    CHECK_EQ(replayed.bookRows, live.bookRows);
    CHECK_EQ(live.gaps, 1u);
    CHECK_EQ(live.epochResets, 1u);
    int resets = 0;
    for (const auto& row : readLines(book)) resets += col(row, 4) == "R";
    CHECK_EQ(resets, 2);
}

TEST(steady_state_does_not_allocate) {
    fs::path dir = freshDir("alloc");
    Metrics m;
    SymbolPipeline p(Venue::Spot, "BTCUSDT", m);
    CHECK(p.open((dir / "md.csv").string(), (dir / "ob.csv").string()));
    Router r(Venue::Spot, m);
    r.add(p);

    // realistic mix: depth5 ahead of its diff, diffs near the top, trades
    std::vector<std::string> frames;
    frames.push_back(depth5Frame(100, BIDS5, ASKS5));
    for (uint64_t id = 101; id < 2101; id++) {
        std::string px = std::to_string(90 + id % 10) + ".00";
        if (id % 5 == 0) frames.push_back(depth5Frame(id, BIDS5, ASKS5));
        frames.push_back(diffFrame(id, id, {{px, id % 3 ? "1.5" : "0"}}, {{"101.00", "1"}}));
        frames.push_back(tradeFrame("100.00", "0.01", id % 2 == 0));
    }
    size_t half = frames.size() / 2;
    uint64_t seq = 0;
    for (size_t i = 0; i < half; i++) r.onMessage(0, 0, seq++, T0, frames[i]);  // warm up
    size_t before = allocationCount();
    for (size_t i = half; i < frames.size(); i++) r.onMessage(0, 0, seq++, T0, frames[i]);
    CHECK_EQ(allocationCount() - before, 0u);
    CHECK_EQ(m.decodeErrors, 0u);
    CHECK(p.sync().live());
    p.close();
}

// ---- command line

TEST(cli_parsing_and_limits) {
    std::vector<std::string> syms;
    std::string err;
    CHECK(parseSymbols("btcusdt,ETHUSDT,btcusdt", syms, err));
    CHECK_EQ(syms.size(), 2u);
    CHECK_EQ(syms[0], std::string("BTCUSDT"));
    CHECK(!parseSymbols("BTC-USDT", syms, err));
    CHECK(!parseSymbols(",,", syms, err));

    Config cfg;
    const char* ok[] = {"binance_capture", "--venue", "usdm", "--symbols=btcusdt", "--duration", "60"};
    CHECK(parseArgs(6, ok, cfg, err) == ParseResult::Run);
    CHECK(cfg.venue == Venue::UsdM);
    CHECK_EQ(cfg.durationSec, 60);

    Config badCfg;
    const char* bad[] = {"binance_capture", "--symbols", "BTCUSDT", "--symbols-per-shard", "500"};
    CHECK(parseArgs(5, bad, badCfg, err) == ParseResult::Error);

    Endpoint ep;
    CHECK(parseWsUrl("wss://stream.binance.com:443/stream", ep, err));
    CHECK_EQ(ep.port, std::string("443"));
    CHECK(!parseWsUrl("ws://stream.binance.com/stream", ep, err));
    CHECK_EQ(streamTarget(defaultEndpoint(Venue::Spot), {"BTCUSDT"}),
             std::string("/stream?streams=btcusdt@depth@100ms/btcusdt@depth5@100ms/btcusdt@trade"));
    CHECK_EQ(streamTarget(defaultEndpoint(Venue::UsdM), {"BTCUSDT"}).rfind("/public/stream?", 0), 0u);

    CHECK_EQ(instrumentId(Venue::Spot, "BTCUSDT"), instrumentId(Venue::Spot, "BTCUSDT"));
    CHECK(instrumentId(Venue::Spot, "BTCUSDT") != instrumentId(Venue::UsdM, "BTCUSDT"));
    CHECK(instrumentId(Venue::Spot, "BTCUSDT") >= 0);
}

int main() {
    int failedTests = 0;
    for (const TestCase& t : allTests()) {
        int before = failures;
        t.fn();
        bool ok = failures == before;
        if (!ok) failedTests++;
        std::printf("[%s] %s\n", ok ? " OK " : "FAIL", t.name);
    }
    std::printf("\n%zu tests, %d failed\n", allTests().size(), failedTests);
    return failedTests == 0 ? 0 : 1;
}
