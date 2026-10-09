#include "decoder.hpp"

#include <string_view>
#include <utility>

#include "decimal.hpp"

namespace capture {

namespace ondemand = simdjson::ondemand;

// parses [["price","qty"], ...] and hands each level to onLevel (which can reject it)
template <typename F>
static DecodeError parseLevels(ondemand::value& v, F&& onLevel) {
    ondemand::array arr;
    if (v.get_array().get(arr)) return DecodeError::Json;
    for (auto entry : arr) {
        ondemand::array pair;
        if (entry.get_array().get(pair)) return DecodeError::Json;
        std::string_view price, qty;
        int k = 0;
        for (auto item : pair) {
            std::string_view s;
            if (item.get_string().get(s)) return DecodeError::Json;
            if (k == 0) price = s;
            else if (k == 1) qty = s;
            k++;
        }
        if (k != 2) return DecodeError::BadLevels;
        Level level;
        if (parseScaled(price, level.price) != DecimalError::None ||
            parseScaled(qty, level.qty) != DecimalError::None)
            return DecodeError::Decimal;
        if (!onLevel(level)) return DecodeError::BadLevels;
    }
    return DecodeError::None;
}

// depth5 levels have to come best first, with qty > 0, and at most 5 of them
template <Side S>
static DecodeError parseTop5(ondemand::value& v, std::array<Level, TOP_N>& out, int& n) {
    n = 0;
    return parseLevels(v, [&](const Level& l) {
        if (n == TOP_N || l.qty <= 0) return false;
        if (n > 0) {
            bool worseThanPrev = S == Side::Bid ? l.price < out[n - 1].price : l.price > out[n - 1].price;
            if (!worseThanPrev) return false;
        }
        out[n++] = l;
        return true;
    });
}

DecodeError decodeDiff(ondemand::object& obj, Venue venue, DepthDiff& out) {
    out.bids.clear();
    out.asks.clear();
    bool hasU = false, hasLastU = false, hasPu = false, hasBids = false, hasAsks = false;
    for (auto f : obj) {
        ondemand::field field;
        if (std::move(f).get(field)) return DecodeError::Json;
        std::string_view key;
        if (field.unescaped_key().get(key)) return DecodeError::Json;

        DecodeError err = DecodeError::None;
        if (key == "U") {
            if (field.value().get_uint64().get(out.firstId)) return DecodeError::Json;
            hasU = true;
        } else if (key == "u") {
            if (field.value().get_uint64().get(out.lastId)) return DecodeError::Json;
            hasLastU = true;
        } else if (key == "pu") {
            if (field.value().get_uint64().get(out.prevLastId)) return DecodeError::Json;
            hasPu = true;
        } else if (key == "b") {
            err = parseLevels(field.value(), [&](const Level& l) { out.bids.push_back(l); return true; });
            hasBids = true;
        } else if (key == "a") {
            err = parseLevels(field.value(), [&](const Level& l) { out.asks.push_back(l); return true; });
            hasAsks = true;
        }
        if (err != DecodeError::None) return err;
    }
    if (!hasU || !hasLastU || !hasBids || !hasAsks) return DecodeError::MissingField;
    if (venue == Venue::UsdM && !hasPu) return DecodeError::MissingField;
    if (out.firstId > out.lastId) return DecodeError::BadIds;
    return DecodeError::None;
}

DecodeError decodeDepth5(ondemand::object& obj, Venue venue, Depth5& out) {
    // spot:    {"lastUpdateId":..,"bids":[..],"asks":[..]}
    // futures: {"e":"depthUpdate",..,"U":..,"u":..,"pu":..,"b":[..],"a":[..]}  (looks like a diff!)
    bool spot = venue == Venue::Spot;
    std::string_view idKey = spot ? "lastUpdateId" : "u";
    std::string_view bidsKey = spot ? "bids" : "b";
    std::string_view asksKey = spot ? "asks" : "a";

    bool hasId = false, hasBids = false, hasAsks = false;
    for (auto f : obj) {
        ondemand::field field;
        if (std::move(f).get(field)) return DecodeError::Json;
        std::string_view key;
        if (field.unescaped_key().get(key)) return DecodeError::Json;

        DecodeError err = DecodeError::None;
        if (key == idKey) {
            if (field.value().get_uint64().get(out.lastId)) return DecodeError::Json;
            hasId = true;
        } else if (key == bidsKey) {
            err = parseTop5<Side::Bid>(field.value(), out.bids, out.nBids);
            hasBids = true;
        } else if (key == asksKey) {
            err = parseTop5<Side::Ask>(field.value(), out.asks, out.nAsks);
            hasAsks = true;
        }
        if (err != DecodeError::None) return err;
    }
    if (!hasId || !hasBids || !hasAsks) return DecodeError::MissingField;
    return DecodeError::None;
}

DecodeError decodeTrade(ondemand::object& obj, Trade& out) {
    bool hasPrice = false, hasQty = false, hasMaker = false;
    for (auto f : obj) {
        ondemand::field field;
        if (std::move(f).get(field)) return DecodeError::Json;
        std::string_view key;
        if (field.unescaped_key().get(key)) return DecodeError::Json;

        if (key == "p" || key == "q") {
            std::string_view s;
            if (field.value().get_string().get(s)) return DecodeError::Json;
            int64_t& dst = key == "p" ? out.price : out.qty;
            if (parseScaled(s, dst) != DecimalError::None) return DecodeError::Decimal;
            (key == "p" ? hasPrice : hasQty) = true;
        } else if (key == "m") {
            if (field.value().get_bool().get(out.buyerIsMaker)) return DecodeError::Json;
            hasMaker = true;
        }
    }
    if (!hasPrice || !hasQty || !hasMaker) return DecodeError::MissingField;
    return DecodeError::None;
}

}  // namespace capture
