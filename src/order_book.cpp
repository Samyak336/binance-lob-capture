#include "order_book.hpp"

#include <algorithm>
#include <iterator>

namespace capture {

static bool hasPrice(const std::vector<Level>* levels, int64_t price) {
    if (!levels) return false;
    for (const Level& l : *levels)
        if (l.price == price) return true;
    return false;
}

// first level whose price is at least as good as `price`
template <Side S>
std::vector<Level>::iterator BookSide<S>::find(int64_t price) {
    return std::lower_bound(levels_.begin(), levels_.end(), price,
                            [](const Level& l, int64_t p) { return worse(l.price, p); });
}

template <Side S>
void BookSide<S>::set(int64_t price, int64_t qty) {
    auto it = find(price);
    bool found = it != levels_.end() && it->price == price;
    if (qty == 0) {
        if (found) levels_.erase(it);
        return;
    }
    if (found) {
        it->qty = qty;
        return;
    }
    levels_.insert(it, Level{price, qty});
    if (size() > MAX_LEVELS + TRIM_SLACK) trim();
}

template <Side S>
void BookSide<S>::trim() {
    size_t drop = levels_.size() - MAX_LEVELS;
    levels_.erase(levels_.begin(), levels_.begin() + static_cast<long>(drop));
    trimmed_ += drop;
    // we forgot everything below the new worst level, so the verified part can't go past it
    int64_t worst = levels_.front().price;
    if (worse(verifiedEdge_, worst)) verifiedEdge_ = worst;
}

template <Side S>
RefreshStats BookSide<S>::replaceTop(const Level* snap, int n, int depth, const std::vector<Level>* skip) {
    RefreshStats stats;
    bool wholeSide = n < depth;
    int64_t edge = wholeSide ? ALL_VERIFIED : snap[n - 1].price;
    auto first = wholeSide ? levels_.begin() : find(edge);

    auto record = [&](int64_t price) {
        if (hasPrice(skip, price)) return;
        if (isVerified(price)) stats.mismatched++;
        else stats.learned++;
    };

    // walk our levels [first, end) from the best end alongside the snapshot
    // (also best first) and count every difference before overwriting
    auto ours = levels_.end();
    int j = 0;
    while (ours != first || j < n) {
        if (ours != first && (j == n || worse(snap[j].price, std::prev(ours)->price))) {
            --ours;  // we have a level the snapshot doesn't
            record(ours->price);
        } else if (ours == first || worse(std::prev(ours)->price, snap[j].price)) {
            record(snap[j].price);  // snapshot has a level we don't
            j++;
        } else {
            --ours;  // same price, compare qty
            if (ours->qty != snap[j].qty) record(snap[j].price);
            j++;
        }
    }

    levels_.erase(first, levels_.end());
    for (int k = n - 1; k >= 0; k--) levels_.push_back(snap[k]);  // worst first to keep the order

    if (wholeSide) verifiedEdge_ = ALL_VERIFIED;
    else if (worse(edge, verifiedEdge_)) verifiedEdge_ = edge;
    return stats;
}

bool OrderBook::topVerified() const {
    for (int i = 0; i < TOP_N; i++)
        if (!bids_.slotVerified(i) || !asks_.slotVerified(i)) return false;
    return true;
}

template class BookSide<Side::Bid>;
template class BookSide<Side::Ask>;

}  // namespace capture
