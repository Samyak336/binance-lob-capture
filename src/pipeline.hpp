#pragma once

#include <cstdint>
#include <string>
#include <string_view>

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

// values for the `type` column
constexpr char ROW_DIFF = 'D';   // depthUpdate applied
constexpr char ROW_DEPTH5 = 'P'; // depth5 seeded or verified the book
constexpr char ROW_TRADE = 'T';  // trade, book unchanged (just an annotation)
constexpr char ROW_RESET = 'R';  // book dropped because of a gap/reconnect, all levels 0

// FNV-1a hash of "venue:SYMBOL" with the sign bit cleared. only depends on the
// instrument, so the live run and the replay always agree on it
int32_t instrumentId(Venue venue, std::string_view symbol);

// Handles one symbol: write the market data row, decode, update the book,
// write the order book row.
// Live mode calls onMessage(), replay calls process() with rows read back from the
// market data csv. The order book side only depends on what's in that csv, which
// is why a replay produces exactly the same order book file.
class SymbolPipeline {
public:
    SymbolPipeline(Venue venue, std::string symbol, Metrics& metrics);

    // empty marketPath = don't write market data (replay)
    bool open(const std::string& marketPath, const std::string& bookPath);

    // payload is the minified inner `data` object, obj is the same object ready to iterate
    void onMessage(const MessageMeta& meta, std::string_view payload, simdjson::ondemand::object& obj);
    void process(const MessageMeta& meta, simdjson::ondemand::object& obj);

    void flush();
    void close();

    Venue venue() const { return venue_; }
    const std::string& symbol() const { return symbol_; }
    const std::string& streamName() const { return streamName_; }
    const BookSync& sync() const { return sync_; }
    const std::string& bookPath() const { return bookCsv_.path(); }
    const std::string& marketPath() const { return marketCsv_.path(); }

private:
    void writeMarketRow(const MessageMeta& meta, std::string_view payload);
    void writeBookRow(const MessageMeta& meta, char type, char side);
    void decodeFailed(const MessageMeta& meta, DecodeError err);

    Venue venue_;
    std::string symbol_;      // BTCUSDT
    std::string streamName_;  // btcusdt (how it appears in stream names)
    int32_t id_;
    Metrics& metrics_;
    BookSync sync_;
    CsvWriter marketCsv_;
    CsvWriter bookCsv_;
    uint64_t seqNo_ = 0;

    // decode targets, reused for every message
    DepthDiff diff_;
    Depth5 depth5_;
    Trade trade_;
};

}  // namespace capture
