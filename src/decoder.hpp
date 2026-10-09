#pragma once

#include <array>
#include <cstdint>
#include <vector>

#include "simdjson.h"
#include "types.hpp"

namespace capture {

// @depth@100ms message. quantities are the new total at that price (not a
// delta) and 0 means the level is gone
struct DepthDiff {
    uint64_t firstId = 0;     // U
    uint64_t lastId = 0;      // u
    uint64_t prevLastId = 0;  // pu, futures only: the previous diff's u
    std::vector<Level> bids;  // reused for every message
    std::vector<Level> asks;
};

// @depth5@100ms message: the full top 5 per side as of update id lastId
struct Depth5 {
    uint64_t lastId = 0;  // spot: lastUpdateId, futures: u
    std::array<Level, TOP_N> bids{};
    std::array<Level, TOP_N> asks{};
    int nBids = 0;
    int nAsks = 0;
};

struct Trade {
    int64_t price = 0;
    int64_t qty = 0;
    bool buyerIsMaker = false;  // "m"
};

enum class DecodeError : uint8_t { None, Json, MissingField, Decimal, BadLevels, BadIds };

constexpr const char* toString(DecodeError e) {
    switch (e) {
        case DecodeError::None: return "ok";
        case DecodeError::Json: return "malformed json";
        case DecodeError::MissingField: return "missing field";
        case DecodeError::Decimal: return "bad or over-precise decimal";
        case DecodeError::BadLevels: return "bad price levels";
        case DecodeError::BadIds: return "U > u";
    }
    return "?";
}

// each of these walks the payload's fields once, in order
DecodeError decodeDiff(simdjson::ondemand::object& obj, Venue venue, DepthDiff& out);
DecodeError decodeDepth5(simdjson::ondemand::object& obj, Venue venue, Depth5& out);
DecodeError decodeTrade(simdjson::ondemand::object& obj, Trade& out);

}  // namespace capture
