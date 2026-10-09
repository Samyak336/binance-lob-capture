#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace capture {

// all prices and quantities are stored as integers in units of 1e-8.
// binance spot never sends more than 8 decimals (futures sends fewer)
constexpr int SCALE_DIGITS = 8;
constexpr int64_t SCALE = 100'000'000;

// levels per side in the order book csv
constexpr int TOP_N = 5;

// biggest REST snapshot binance gives (spot limit=5000, futures stops at 1000)
constexpr int MAX_SNAPSHOT_LEVELS = 5000;

enum class Venue : uint8_t { Spot, UsdM };
enum class StreamKind : uint8_t { DepthDiff, Depth5, Trade };
enum class Side : uint8_t { Bid, Ask };

// where a book that isn't live gets its starting state from
//   Rest   - GET /api/v3/depth (or /fapi/v1/depth), the way binance's docs describe it
//   Depth5 - the next depth5 message. no REST calls, but at first only 5 levels are known
enum class SeedMode : uint8_t { Rest, Depth5 };

constexpr std::string_view toString(Venue v) { return v == Venue::Spot ? "spot" : "usdm"; }
constexpr std::string_view toString(SeedMode m) { return m == SeedMode::Rest ? "rest" : "depth5"; }

constexpr std::string_view toString(StreamKind k) {
    switch (k) {
        case StreamKind::DepthDiff: return "depth_diff";
        case StreamKind::Depth5: return "depth5";
        case StreamKind::Trade: return "trade";
    }
    return "?";
}

constexpr bool parseVenue(std::string_view s, Venue& out) {
    if (s == "spot") { out = Venue::Spot; return true; }
    if (s == "usdm") { out = Venue::UsdM; return true; }
    return false;
}

constexpr bool parseStreamKind(std::string_view s, StreamKind& out) {
    if (s == "depth_diff") { out = StreamKind::DepthDiff; return true; }
    if (s == "depth5") { out = StreamKind::Depth5; return true; }
    if (s == "trade") { out = StreamKind::Trade; return true; }
    return false;
}

constexpr bool parseSeedMode(std::string_view s, SeedMode& out) {
    if (s == "rest") { out = SeedMode::Rest; return true; }
    if (s == "depth5") { out = SeedMode::Depth5; return true; }
    return false;
}

struct Level {
    int64_t price = 0;
    int64_t qty = 0;

    bool operator==(const Level& o) const { return price == o.price && qty == o.qty; }
};

// everything about a message except the payload. live mode fills this from the
// socket, replay fills it from a csv row - the book code can't tell the difference
struct MessageMeta {
    int64_t recvSec = 0;
    int32_t recvNsec = 0;
    Venue venue = Venue::Spot;
    StreamKind kind = StreamKind::DepthDiff;
    uint32_t shardId = 0;
    uint64_t connEpoch = 0;
    uint64_t connSeq = 0;
};

// a REST snapshot doesn't come from the socket, so it has no conn_seq of its own.
// instead we remember which market data row it came right after (same shard and
// epoch, conn_seq = afterConnSeq) - that's where replay puts it back
struct SnapshotMeta {
    int64_t recvSec = 0;
    int32_t recvNsec = 0;
    uint32_t shardId = 0;
    uint64_t connEpoch = 0;
    uint64_t afterConnSeq = 0;
    int depthLimit = 0;  // the limit= it was requested with
};

}  // namespace capture
