#pragma once

#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_map>

#include "metrics.hpp"
#include "pipeline.hpp"
#include "simdjson.h"

namespace capture {

// simdjson needs SIMDJSON_PADDING readable bytes after the json, so frames get
// copied in here first. it only ever grows
class PaddedBuffer {
public:
    std::string_view assign(std::string_view bytes);
    char* reserve(size_t n);  // room for n bytes + padding, old contents not kept
    size_t capacity() const { return capacity_; }

private:
    std::unique_ptr<char[]> data_;
    size_t capacity_ = 0;
};

// Takes raw frames like {"stream":"btcusdt@depth@100ms","data":{...}} and sends
// them to the right symbol.
// Each frame is parsed once: read "stream", grab the raw bytes of "data" (that's
// payload_json, no re-serialising), then rewind that same object and let the
// pipeline decode it.
class Router {
public:
    Router(Venue venue, Metrics& metrics);

    void add(SymbolPipeline& pipeline);
    void onMessage(uint32_t shardId, uint64_t connEpoch, uint64_t connSeq, int64_t recvNs, std::string_view frame);

private:
    bool resolve(std::string_view stream, SymbolPipeline*& pipeline, StreamKind& kind) const;
    void badFrame(const char* why, std::string_view frame);

    Venue venue_;
    Metrics& metrics_;
    simdjson::ondemand::parser parser_;
    PaddedBuffer frame_;
    PaddedBuffer minified_;
    std::unordered_map<std::string_view, SymbolPipeline*> routes_;  // key = pipeline's streamName()
};

}  // namespace capture
