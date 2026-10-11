#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include "book_sync.hpp"
#include "csv.hpp"
#include "decoder.hpp"
#include "metrics.hpp"
#include "types.hpp"

namespace capture {

constexpr std::string_view MARKET_HEADER =
    "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json";

constexpr std::string_view BOOK_HEADER =
    "tsec,tnsec,seqNo,id,type,side,"
    "bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,"
    "ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4";

// REST snapshots (rest mode only), so replay doesn't need the network
constexpr std::string_view SNAPSHOT_HEADER =
    "recv_tsec,recv_tnsec,venue,shard_id,conn_epoch,after_conn_seq,symbol,depth_limit,payload_json";

// values for the `type` column
constexpr char ROW_DIFF = 'D';   // depthUpdate applied
constexpr char ROW_DEPTH5 = 'P'; // depth5 applied (checked + written over the top 5), or seeded the book
constexpr char ROW_REST = 'S';   // REST snapshot seeded the book
constexpr char ROW_TRADE = 'T';  // trade, book unchanged (just an annotation)
constexpr char ROW_RESET = 'R';  // book dropped because of a gap/reconnect, all levels 0

constexpr char rowType(BookChange change) {
    switch (change) {
        case BookChange::RestSeed: return ROW_REST;
        case BookChange::Depth5: return ROW_DEPTH5;
        case BookChange::Diff: return ROW_DIFF;
        case BookChange::Reset: return ROW_RESET;
    }
    return '?';
}

// FNV-1a hash of "venue:SYMBOL" with the sign bit cleared. only depends on the
// instrument, so the live run and the replay always agree on it
int32_t instrumentId(Venue venue, std::string_view symbol);

// Handles one symbol: write the market data row, decode, update the book,
// write the order book row.
// Live mode calls onMessage() / onRestSnapshot(), replay calls process() /
// processRestSnapshot() with rows read back from the csv files. The order book
// side only depends on what's in those files, which is why a replay produces
// exactly the same order book file.
class SymbolPipeline {
public:
    SymbolPipeline(Venue venue, SeedMode seedMode, std::string symbol, Metrics& metrics);
    // the book writes its rows through a callback that points back at this object
    SymbolPipeline(const SymbolPipeline&) = delete;
    SymbolPipeline& operator=(const SymbolPipeline&) = delete;

    // empty path = don't write that file (replay only writes the order book)
    bool open(const std::string& marketPath, const std::string& bookPath, const std::string& snapshotPath);

    // payload is the minified inner `data` object, obj is the same object ready to iterate
    void onMessage(const MessageMeta& meta, std::string_view payload, simdjson::ondemand::object& obj);
    void process(const MessageMeta& meta, simdjson::ondemand::object& obj);

    // a REST snapshot arrived (already decoded). it's saved together with this
    // symbol's last market data row, which is where replay applies it again
    void onRestSnapshot(int64_t recvNs, int depthLimit, std::string_view payload, const RestSnapshot& snap);
    void processRestSnapshot(const SnapshotMeta& meta, const RestSnapshot& snap);

    // live mode hooks this up to the REST client. called (at most once until the
    // answer comes back) when the book needs a snapshot
    void setSnapshotRequester(std::function<void(SymbolPipeline&)> requester) { requester_ = std::move(requester); }

    void flush();
    void close();

    Venue venue() const { return venue_; }
    const std::string& symbol() const { return symbol_; }
    const std::string& streamName() const { return streamName_; }
    const BookSync& sync() const { return sync_; }
    const std::string& bookPath() const { return bookCsv_.path(); }
    const std::string& marketPath() const { return marketCsv_.path(); }
    uint64_t writeErrors() const {
        return marketCsv_.writeErrors() + bookCsv_.writeErrors() + snapshotCsv_.writeErrors();
    }

private:
    void writeMarketRow(const MessageMeta& meta, std::string_view payload);
    void writeSnapshotRow(const SnapshotMeta& meta, std::string_view payload);
    void writeBookRow(char type, char side);
    void decodeFailed(const MessageMeta& meta, DecodeError err);
    void maybeRequestSnapshot();

    Venue venue_;
    std::string symbol_;      // BTCUSDT
    std::string streamName_;  // btcusdt (how it appears in stream names)
    int32_t id_;
    Metrics& metrics_;
    BookSync sync_;
    CsvWriter marketCsv_;
    CsvWriter bookCsv_;
    CsvWriter snapshotCsv_{0};  // a few big rows per run, written straight away
    uint64_t seqNo_ = 0;

    // timestamp for the order book rows of the event being processed: the message's
    // receive time, or the time a REST snapshot arrived
    int64_t rowSec_ = 0;
    int32_t rowNsec_ = 0;

    MessageMeta last_;  // last market data message, the anchor for REST snapshots
    std::function<void(SymbolPipeline&)> requester_;
    bool snapshotRequested_ = false;

    // decode targets, reused for every message
    DepthDiff diff_;
    Depth5 depth5_;
    Trade trade_;
};

}  // namespace capture
