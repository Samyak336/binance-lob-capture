#pragma once

#include <cstdint>
#include <functional>
#include <vector>

#include "decoder.hpp"
#include "metrics.hpp"
#include "order_book.hpp"

namespace capture {

// every change the book goes through, in order. the pipeline writes one order book row for each
enum class BookChange : uint8_t {
    RestSeed,  // a REST snapshot started the book
    Depth5,    // a depth5 was laid on the book (or, with --snapshot depth5, started it)
    Diff,      // a depthUpdate was applied
    Reset,     // the book was thrown away (gap or reconnect)
};

// Keeps the order book in step with binance's update ids.
//
// not live (startup, after a gap, after a reconnect):
//   diffs are buffered. a snapshot seeds the book - a REST snapshot by default,
//   or the next depth5 with --snapshot depth5. buffered diffs the snapshot already
//   covers are dropped, the rest are applied on top.
// live:
//   every diff must continue the sequence (spot: U <= last_u + 1, futures: pu == last_u).
//   if not, that's a gap -> drop the book and seed again.
// depth5 while live:
//   used to check/repair the top of the book. Its id is usually ahead of the book
//   (on spot it tends to arrive just before its matching diff, on futures its id
//   falls inside a diff's range), so we hold it and apply it right before the diff
//   that covers its id.
class BookSync {
public:
    static constexpr int MAX_BUFFERED = 256;

    // called right after each change, while the book is in that state.
    // side: 'B' only bids changed, 'S' only asks changed, 'N' both or not applicable
    using Listener = std::function<void(BookChange change, char side)>;

    BookSync(Venue venue, SeedMode seedMode, Metrics& metrics, Listener listener = {});

    void onEpoch(uint64_t epoch);
    void onDiff(const DepthDiff& diff);
    void onDepth5(const Depth5& snap);
    // depthLimit = the limit= the snapshot was requested with. fewer levels than
    // that means we got the whole side
    void onRestSnapshot(const RestSnapshot& snap, int depthLimit);

    // rest mode: diffs are waiting and only a REST snapshot can start the book
    bool needsSnapshot() const { return seedMode_ == SeedMode::Rest && !live_ && ringSize_ > 0; }

    bool live() const { return live_; }
    const OrderBook& book() const { return book_; }
    uint64_t lastUpdateId() const { return lastId_; }

private:
    enum class DiffResult : uint8_t { Applied, Stale, Gap };

    DiffResult apply(const DepthDiff& diff);
    bool continues(const DepthDiff& diff, uint64_t last, bool afterSnapshot) const;
    void refresh(const Depth5& snap, const DepthDiff* nextDiff);
    void seed(uint64_t lastId, const Level* bids, int nBids, const Level* asks, int nAsks, int depth,
              BookChange change);
    void dropBook(bool keepBuffer);
    void bufferDiff(const DepthDiff& diff);
    void popBuffered();
    void changed(BookChange change, char side = 'N') {
        if (listener_) listener_(change, side);
    }

    Venue venue_;
    SeedMode seedMode_;
    Metrics& metrics_;
    Listener listener_;
    OrderBook book_;

    bool live_ = false;
    bool justSeeded_ = false;  // futures: the first diff after a seed can straddle the snapshot id
    uint64_t lastId_ = 0;

    bool haveEpoch_ = false;
    uint64_t epoch_ = 0;

    bool hasPending_ = false;
    Depth5 pending_;

    // ring buffer for diffs that arrive while we're not live. slots keep their
    // vector capacity so this doesn't allocate after warmup
    std::vector<DepthDiff> ring_;
    int ringHead_ = 0;
    int ringSize_ = 0;
};

}  // namespace capture
