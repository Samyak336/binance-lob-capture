#include "book_sync.hpp"

namespace capture {

BookSync::BookSync(Venue venue, Metrics& metrics) : venue_(venue), metrics_(metrics), ring_(MAX_BUFFERED) {}

BookEffect BookSync::onEpoch(uint64_t epoch) {
    if (haveEpoch_ && epoch == epoch_) return BookEffect::None;
    bool firstConnection = !haveEpoch_;
    haveEpoch_ = true;
    epoch_ = epoch;
    if (firstConnection) return BookEffect::None;

    // new connection - no idea what we missed while disconnected, start over
    metrics_.epochResets++;
    bool wasLive = live_;
    dropBook();
    return wasLive ? BookEffect::Reset : BookEffect::None;
}

BookEffect BookSync::onDiff(const DepthDiff& diff) {
    if (!live_) {
        bufferDiff(diff);
        return BookEffect::None;
    }
    DiffResult r = apply(diff);
    if (r == DiffResult::Applied) return BookEffect::Applied;
    if (r == DiffResult::Stale) return BookEffect::None;

    metrics_.gaps++;
    dropBook();
    bufferDiff(diff);  // the next snapshot might still connect to this one
    return BookEffect::Reset;
}

BookEffect BookSync::onDepth5(const Depth5& snap) {
    if (!live_) return seed(snap) ? BookEffect::Applied : BookEffect::None;

    if (snap.lastId == lastId_) {
        refresh(snap, nullptr);
        return BookEffect::Applied;
    }
    if (snap.lastId < lastId_) {
        metrics_.depth5Stale++;
        return BookEffect::None;
    }
    // ahead of the diffs: keep it until the diff that covers its id shows up
    if (hasPending_) metrics_.depth5Superseded++;
    pending_ = snap;
    hasPending_ = true;
    return BookEffect::None;
}

BookSync::DiffResult BookSync::apply(const DepthDiff& diff) {
    if (diff.lastId <= lastId_) {
        metrics_.diffsStale++;
        return DiffResult::Stale;
    }
    bool continuous;
    if (venue_ == Venue::Spot)
        continuous = diff.firstId <= lastId_ + 1;
    else
        continuous = justSeeded_ ? diff.prevLastId <= lastId_ : diff.prevLastId == lastId_;
    if (!continuous) return DiffResult::Gap;

    // A held depth5 with id L in (last_u, u] is a state somewhere inside this
    // diff. Put it on the book first, then apply the diff: every level the diff
    // touches ends up at its value as of u, and the levels it doesn't touch
    // didn't change between L and u, so the snapshot values are already right.
    if (hasPending_ && pending_.lastId <= diff.lastId) {
        refresh(pending_, &diff);
        hasPending_ = false;
    }

    for (const Level& l : diff.bids) book_.bids().set(l.price, l.qty);
    for (const Level& l : diff.asks) book_.asks().set(l.price, l.qty);
    lastId_ = diff.lastId;
    justSeeded_ = false;
    metrics_.diffsApplied++;
    return DiffResult::Applied;
}

void BookSync::refresh(const Depth5& snap, const DepthDiff* nextDiff) {
    RefreshStats b = book_.bids().replaceTop(snap.bids.data(), snap.nBids, TOP_N, nextDiff ? &nextDiff->bids : nullptr);
    RefreshStats a = book_.asks().replaceTop(snap.asks.data(), snap.nAsks, TOP_N, nextDiff ? &nextDiff->asks : nullptr);
    metrics_.depth5Checks++;
    metrics_.levelsMismatched += static_cast<uint64_t>(b.mismatched + a.mismatched);
    metrics_.levelsLearned += static_cast<uint64_t>(b.learned + a.learned);
}

bool BookSync::seed(const Depth5& snap) {
    book_.clear();
    book_.bids().replaceTop(snap.bids.data(), snap.nBids, TOP_N, nullptr);
    book_.asks().replaceTop(snap.asks.data(), snap.nAsks, TOP_N, nullptr);
    lastId_ = snap.lastId;
    live_ = true;
    justSeeded_ = true;
    hasPending_ = false;

    // replay whatever came in while we were waiting. anything the snapshot
    // already covers comes back as Stale and just gets dropped
    while (ringSize_ > 0) {
        if (apply(ring_[static_cast<size_t>(ringHead_)]) == DiffResult::Gap) {
            // snapshot is older than the buffered diffs and doesn't connect -
            // keep them and wait for a newer depth5
            metrics_.seedFailures++;
            live_ = false;
            justSeeded_ = false;
            book_.clear();
            return false;
        }
        popBuffered();
    }
    metrics_.seeds++;
    return true;
}

void BookSync::dropBook() {
    live_ = false;
    justSeeded_ = false;
    hasPending_ = false;
    book_.clear();
    ringSize_ = 0;
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
