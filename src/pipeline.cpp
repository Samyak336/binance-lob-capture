#include "pipeline.hpp"

#include <cctype>
#include <cinttypes>
#include <utility>

#include "log.hpp"

namespace capture {

int32_t instrumentId(Venue venue, std::string_view symbol) {
    uint32_t h = 2166136261u;
    auto mix = [&h](std::string_view s) {
        for (char c : s) {
            h ^= static_cast<unsigned char>(c);
            h *= 16777619u;
        }
    };
    mix(toString(venue));
    mix(":");
    mix(symbol);
    return static_cast<int32_t>(h & 0x7fffffffu);
}

SymbolPipeline::SymbolPipeline(Venue venue, SeedMode seedMode, std::string symbol, Metrics& metrics)
    : venue_(venue),
      symbol_(std::move(symbol)),
      streamName_(symbol_),
      id_(instrumentId(venue, symbol_)),
      metrics_(metrics),
      // every change to the book gets its row right away, while the book is in that state
      sync_(venue, seedMode, metrics, [this](BookChange change, char side) { writeBookRow(rowType(change), side); }) {
    for (char& c : streamName_) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    diff_.bids.reserve(1024);
    diff_.asks.reserve(1024);
}

bool SymbolPipeline::open(const std::string& marketPath, const std::string& bookPath, const std::string& snapshotPath) {
    if (!marketPath.empty() && !marketCsv_.open(marketPath, MARKET_HEADER)) return false;
    if (!snapshotPath.empty() && !snapshotCsv_.open(snapshotPath, SNAPSHOT_HEADER)) return false;
    return bookCsv_.open(bookPath, BOOK_HEADER);
}

void SymbolPipeline::onMessage(const MessageMeta& meta, std::string_view payload, simdjson::ondemand::object& obj) {
    writeMarketRow(meta, payload);
    process(meta, obj);
}

void SymbolPipeline::process(const MessageMeta& meta, simdjson::ondemand::object& obj) {
    last_ = meta;
    rowSec_ = meta.recvSec;  // every row this message causes gets its receive time
    rowNsec_ = meta.recvNsec;
    sync_.onEpoch(meta.connEpoch);

    if (meta.kind == StreamKind::DepthDiff) {
        DecodeError err = decodeDiff(obj, venue_, diff_);
        if (err != DecodeError::None) return decodeFailed(meta, err);
        sync_.onDiff(diff_);
        maybeRequestSnapshot();
    } else if (meta.kind == StreamKind::Depth5) {
        DecodeError err = decodeDepth5(obj, venue_, depth5_);
        if (err != DecodeError::None) return decodeFailed(meta, err);
        sync_.onDepth5(depth5_);
    } else {
        DecodeError err = decodeTrade(obj, trade_);
        if (err != DecodeError::None) return decodeFailed(meta, err);
        metrics_.trades++;
        // side = which side of the book got hit. buyer is maker means a resting
        // bid was hit by a seller -> 'B'
        if (sync_.live()) writeBookRow(ROW_TRADE, trade_.buyerIsMaker ? 'B' : 'S');
    }
}

void SymbolPipeline::onRestSnapshot(int64_t recvNs, int depthLimit, std::string_view payload,
                                    const RestSnapshot& snap) {
    snapshotRequested_ = false;
    SnapshotMeta meta;
    meta.recvSec = recvNs / 1'000'000'000;
    meta.recvNsec = static_cast<int32_t>(recvNs % 1'000'000'000);
    meta.shardId = last_.shardId;
    meta.connEpoch = last_.connEpoch;
    meta.afterConnSeq = last_.connSeq;
    meta.depthLimit = depthLimit;
    writeSnapshotRow(meta, payload);
    processRestSnapshot(meta, snap);
}

void SymbolPipeline::processRestSnapshot(const SnapshotMeta& meta, const RestSnapshot& snap) {
    metrics_.snapshotsReceived++;
    rowSec_ = meta.recvSec;  // the S row and the buffered diffs applied with it
    rowNsec_ = meta.recvNsec;
    sync_.onRestSnapshot(snap, meta.depthLimit);
    // if it was older than the buffered diffs we need another one (binance docs: "go back to step 3")
    maybeRequestSnapshot();
}

void SymbolPipeline::maybeRequestSnapshot() {
    if (!requester_ || snapshotRequested_ || !sync_.needsSnapshot()) return;
    snapshotRequested_ = true;
    requester_(*this);
}

void SymbolPipeline::writeMarketRow(const MessageMeta& meta, std::string_view payload) {
    if (!marketCsv_.isOpen()) return;
    OutBuffer& out = marketCsv_.row();
    out.appendInt(meta.recvSec);
    out.push(',');
    out.appendInt(meta.recvNsec);
    out.push(',');
    out.append(toString(meta.venue));
    out.push(',');
    out.append(toString(meta.kind));
    out.push(',');
    out.appendUint(meta.shardId);
    out.push(',');
    out.appendUint(meta.connEpoch);
    out.push(',');
    out.appendUint(meta.connSeq);
    out.push(',');
    out.append(symbol_);
    out.push(',');
    appendCsvField(out, payload);
    marketCsv_.endRow();
    metrics_.marketRows++;
}

void SymbolPipeline::writeSnapshotRow(const SnapshotMeta& meta, std::string_view payload) {
    if (!snapshotCsv_.isOpen()) return;
    OutBuffer& out = snapshotCsv_.row();
    out.appendInt(meta.recvSec);
    out.push(',');
    out.appendInt(meta.recvNsec);
    out.push(',');
    out.append(toString(venue_));
    out.push(',');
    out.appendUint(meta.shardId);
    out.push(',');
    out.appendUint(meta.connEpoch);
    out.push(',');
    out.appendUint(meta.afterConnSeq);
    out.push(',');
    out.append(symbol_);
    out.push(',');
    out.appendInt(meta.depthLimit);
    out.push(',');
    appendCsvField(out, payload);
    snapshotCsv_.endRow();
    snapshotCsv_.flush();  // rare and big, no point keeping it in memory
}

void SymbolPipeline::writeBookRow(char type, char side) {
    OutBuffer& out = bookCsv_.row();
    out.appendInt(rowSec_);
    out.push(',');
    out.appendInt(rowNsec_);
    out.push(',');
    out.appendUint(++seqNo_);
    out.push(',');
    out.appendInt(id_);
    out.push(',');
    out.push(type);
    out.push(',');
    out.push(side);

    // a book that isn't live is empty, so reset rows come out as all zeros
    const OrderBook& book = sync_.book();
    const auto& bids = book.bids();
    const auto& asks = book.asks();
    for (int i = 0; i < TOP_N; i++) { out.push(','); out.appendInt(i < bids.size() ? bids.best(i).price : 0); }
    for (int i = 0; i < TOP_N; i++) { out.push(','); out.appendInt(i < bids.size() ? bids.best(i).qty : 0); }
    for (int i = 0; i < TOP_N; i++) { out.push(','); out.appendInt(i < asks.size() ? asks.best(i).price : 0); }
    for (int i = 0; i < TOP_N; i++) { out.push(','); out.appendInt(i < asks.size() ? asks.best(i).qty : 0); }
    bookCsv_.endRow();
    metrics_.bookRows++;
    if (sync_.live() && !book.topVerified()) metrics_.uncertainRows++;
}

void SymbolPipeline::decodeFailed(const MessageMeta& meta, DecodeError err) {
    metrics_.decodeErrors++;
    if (metrics_.decodeErrors <= 20) {  // don't flood the log
        logMsg(LogLevel::Warn, "%s %s: can't decode %s payload (epoch %" PRIu64 " seq %" PRIu64 "): %s",
               toString(venue_).data(), symbol_.c_str(), toString(meta.kind).data(), meta.connEpoch, meta.connSeq,
               toString(err));
    }
}

void SymbolPipeline::flush() {
    marketCsv_.flush();
    bookCsv_.flush();
}

void SymbolPipeline::close() {
    marketCsv_.close();
    bookCsv_.close();
    snapshotCsv_.close();
    metrics_.writeErrors += marketCsv_.writeErrors() + bookCsv_.writeErrors() + snapshotCsv_.writeErrors();
}

}  // namespace capture
