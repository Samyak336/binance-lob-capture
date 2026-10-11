#include "book_sync.hpp"

#include <utility>

namespace capture {

static char diffSide(const DepthDiff& d) {
    if (!d.bids.empty() && d.asks.empty()) return 'B';
    if (d.bids.empty() && !d.asks.empty()) return 'S';
    return 'N';
}

BookSync::BookSync(Venue venue, SeedMode seedMode, Metrics& metrics, Listener listener)
    : venue_(venue), seedMode_(seedMode), metrics_(metrics), listener_(std::move(listener)), ring_(MAX_BUFFERED) {}

void BookSync::onEpoch(uint64_t epoch) {
    if (haveEpoch_ && epoch == epoch_) return;
    bool firstConnection = !haveEpoch_;
    haveEpoch_ = true;
    epoch_ = epoch;
    if (firstConnection) return;

    // new connection - no idea what we missed while disconnected, start over
    metrics_.epochResets++;
    bool wasLive = live_;
    dropBook(false);
    if (wasLive) changed(BookChange::Reset);
}

void BookSync::onDiff(const DepthDiff& diff) {
    if (!live_) {
        bufferDiff(diff);
        return;
    }
    if (apply(diff) != DiffResult::Gap) return;

    metrics_.gaps++;
    dropBook(false);
    changed(BookChange::Reset);
    bufferDiff(diff);  // the next snapshot might still connect to this one
}

void BookSync::onDepth5(const Depth5& snap) {
    if (!live_) {
        if (seedMode_ == SeedMode::Rest) {  // only the REST snapshot starts the book, depth5 just checks it later
            metrics_.depth5Ignored++;
            return;
        }
        seed(snap.lastId, snap.bids.data(), snap.nBids, snap.asks.data(), snap.nAsks, TOP_N, BookChange::Depth5);
        return;
    }

    if (snap.lastId == lastId_) {
        refresh(snap, nullptr);
        changed(BookChange::Depth5);
        return;
    }
    if (snap.lastId < lastId_) {
        metrics_.depth5Stale++;
        return;
    }
    // ahead of the diffs: keep it until the diff that covers its id shows up
    if (hasPending_) metrics_.depth5Superseded++;
    pending_ = snap;
    hasPending_ = true;
}

void BookSync::onRestSnapshot(const RestSnapshot& snap, int depthLimit) {
    // we only ask for one while the book isn't live, so this would be a late answer
    if (live_) {
        metrics_.snapshotsUnused++;
        return;
    }
    seed(snap.lastId, snap.bids.data(), static_cast<int>(snap.bids.size()), snap.asks.data(),
         static_cast<int>(snap.asks.size()), depthLimit, BookChange::RestSeed);
}

// does this diff carry on from update id `last`? right after a snapshot a futures
// diff only has to straddle it (pu <= last < u)
bool BookSync::continues(const DepthDiff& diff, uint64_t last, bool afterSnapshot) const {
    if (venue_ == Venue::Spot) return diff.firstId <= last + 1;
    return afterSnapshot ? diff.prevLastId <= last : diff.prevLastId == last;
}

BookSync::DiffResult BookSync::apply(const DepthDiff& diff) {
    if (diff.lastId <= lastId_) {
        metrics_.diffsStale++;
        return DiffResult::Stale;
    }
    if (!continues(diff, lastId_, justSeeded_)) return DiffResult::Gap;

    // A held depth5 with id L in (last_u, u] is a state somewhere inside this
    // diff. Put it on the book first, then apply the diff: every level the diff
    // touches ends up at its value as of u, and the levels it doesn't touch
    // didn't change between L and u, so the snapshot values are already right.
    // It gets its own row in between: the top 5 as of L.
    if (hasPending_ && pending_.lastId <= diff.lastId) {
        refresh(pending_, &diff);
        hasPending_ = false;
        changed(BookChange::Depth5);
    }

    for (const Level& l : diff.bids) book_.bids().set(l.price, l.qty);
    for (const Level& l : diff.asks) book_.asks().set(l.price, l.qty);
    lastId_ = diff.lastId;
    justSeeded_ = false;
    metrics_.diffsApplied++;
    changed(BookChange::Diff, diffSide(diff));
    return DiffResult::Applied;
}

void BookSync::refresh(const Depth5& snap, const DepthDiff* nextDiff) {
    RefreshStats b = book_.bids().replaceTop(snap.bids.data(), snap.nBids, TOP_N, nextDiff ? &nextDiff->bids : nullptr);
    RefreshStats a = book_.asks().replaceTop(snap.asks.data(), snap.nAsks, TOP_N, nextDiff ? &nextDiff->asks : nullptr);
    metrics_.depth5Checks++;
    metrics_.levelsMismatched += static_cast<uint64_t>(b.mismatched + a.mismatched);
    metrics_.levelsLearned += static_cast<uint64_t>(b.learned + a.learned);
}

// same steps for a depth5 (depth 5) and a REST snapshot (depth = its limit)
void BookSync::seed(uint64_t lastId, const Level* bids, int nBids, const Level* asks, int nAsks, int depth,
                    BookChange change) {
    // buffered diffs the snapshot already covers are dropped. the first one left has
    // to connect to it, otherwise the snapshot is older than the buffer: keep the
    // buffer and wait for a newer snapshot
    while (ringSize_ > 0 && ring_[static_cast<size_t>(ringHead_)].lastId <= lastId) {
        metrics_.diffsStale++;
        popBuffered();
    }
    if (ringSize_ > 0 && !continues(ring_[static_cast<size_t>(ringHead_)], lastId, true)) {
        metrics_.seedFailures++;
        return;
    }

    book_.clear();
    book_.bids().replaceTop(bids, nBids, depth, nullptr);
    book_.asks().replaceTop(asks, nAsks, depth, nullptr);
    lastId_ = lastId;
    live_ = true;
    justSeeded_ = true;
    hasPending_ = false;
    metrics_.seeds++;
    changed(change);

    // then the rest of the buffer on top, each diff with its own row
    while (ringSize_ > 0) {
        if (apply(ring_[static_cast<size_t>(ringHead_)]) == DiffResult::Gap) {
            // can't happen with diffs from one connection, but if the buffer has a
            // hole it's a gap like any other. what's left of the buffer stays for the next snapshot
            metrics_.gaps++;
            dropBook(true);
            changed(BookChange::Reset);
            return;
        }
        popBuffered();
    }
}

void BookSync::dropBook(bool keepBuffer) {
    live_ = false;
    justSeeded_ = false;
    hasPending_ = false;
    book_.clear();
    if (!keepBuffer) ringSize_ = 0;
}

void BookSync::bufferDiff(const DepthDiff& diff) {
    if (ringSize_ == MAX_BUFFERED) {
        popBuffered();
        metrics_.bufferOverflows++;
    }
    ring_[static_cast<size_t>((ringHead_ + ringSize_) % MAX_BUFFERED)] = diff;  // copy-assign reuses capacity
    ringSize_++;
    metrics_.diffsBuffered++;
}

void BookSync::popBuffered() {
    ringHead_ = (ringHead_ + 1) % MAX_BUFFERED;
    ringSize_--;
}

}  // namespace capture
