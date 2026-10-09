#pragma once

#include <cstdint>
#include <limits>
#include <vector>

#include "types.hpp"

namespace capture {

// what happened when a depth5 snapshot was laid over the book
struct RefreshStats {
    int mismatched = 0;  // level differed inside the part we thought we knew exactly -> bug somewhere
    int learned = 0;     // level differed outside it -> fine, we just hadn't seen it before
};

// One side of the book: a vector sorted from worst price to best, so the best
// level is at the back.
//
// Why a vector and not std::map: almost all updates happen near the top of the
// book, i.e. near the back of the vector, so an insert/erase only shifts a few
// elements. The top 5 are just the last 5 elements, and there's no allocation
// per level like a map node. Lookup is still a binary search.
//
// It also remembers how deep the book is "verified". A depth5 snapshot tells us
// exactly which levels exist down to its 5th price, and diffs keep that part
// exact. Below that edge we only know levels some diff happened to touch.
template <Side S>
class BookSide {
public:
    // memory cap. we let it go a bit over and then trim the worst levels in one go
    static constexpr int MAX_LEVELS = 5000;
    static constexpr int TRIM_SLACK = 1000;

    BookSide() { levels_.reserve(MAX_LEVELS + TRIM_SLACK + 1); }

    // is price a worse than price b on this side?
    static constexpr bool worse(int64_t a, int64_t b) {
        if constexpr (S == Side::Bid) return a < b;
        else return a > b;
    }

    // qty is the new total at that price, 0 means remove it.
    // removing a level we don't have is fine (binance docs say this happens)
    void set(int64_t price, int64_t qty);

    // overwrite everything at or better than the snapshot's last price with the
    // snapshot (levels are best first). if the snapshot has fewer than `depth`
    // levels it showed the whole side, so the whole side gets replaced.
    // prices in `skip` are left out of the stats because the next diff overwrites them anyway
    RefreshStats replaceTop(const Level* levels, int n, int depth, const std::vector<Level>* skip);

    void clear() {
        levels_.clear();
        verifiedEdge_ = NOTHING_VERIFIED;
    }

    int size() const { return static_cast<int>(levels_.size()); }
    bool empty() const { return levels_.empty(); }
    const Level& best(int i) const { return levels_[levels_.size() - 1 - static_cast<size_t>(i)]; }

    // is slot i (0 = best) backed by a snapshot? an empty slot only counts if
    // we know the side really has fewer levels
    bool slotVerified(int i) const {
        if (i < size()) return !worse(best(i).price, verifiedEdge_);
        return verifiedEdge_ == ALL_VERIFIED;
    }

    uint64_t trimmed() const { return trimmed_; }

private:
    // edge values that mean "no price is verified" / "every price is verified"
    static constexpr int64_t NOTHING_VERIFIED =
        S == Side::Bid ? std::numeric_limits<int64_t>::max() : std::numeric_limits<int64_t>::min();
    static constexpr int64_t ALL_VERIFIED =
        S == Side::Bid ? std::numeric_limits<int64_t>::min() : std::numeric_limits<int64_t>::max();

    std::vector<Level>::iterator find(int64_t price);
    bool isVerified(int64_t price) const { return !worse(price, verifiedEdge_); }
    void trim();

    std::vector<Level> levels_;
    int64_t verifiedEdge_ = NOTHING_VERIFIED;
    uint64_t trimmed_ = 0;
};

class OrderBook {
public:
    BookSide<Side::Bid>& bids() { return bids_; }
    BookSide<Side::Ask>& asks() { return asks_; }
    const BookSide<Side::Bid>& bids() const { return bids_; }
    const BookSide<Side::Ask>& asks() const { return asks_; }

    void clear() {
        bids_.clear();
        asks_.clear();
    }

    // true if all top-5 slots on both sides are snapshot-verified
    bool topVerified() const;

private:
    BookSide<Side::Bid> bids_;
    BookSide<Side::Ask> asks_;
};

}  // namespace capture
