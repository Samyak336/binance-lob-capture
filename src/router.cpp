#include "router.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>

#include "log.hpp"

namespace capture {

namespace ondemand = simdjson::ondemand;

char* PaddedBuffer::reserve(size_t n) {
    size_t need = n + simdjson::SIMDJSON_PADDING;
    if (need > capacity_) {
        size_t cap = std::max(need, capacity_ * 2);
        data_ = std::make_unique<char[]>(cap);
        capacity_ = cap;
    }
    return data_.get();
}

std::string_view PaddedBuffer::assign(std::string_view bytes) {
    char* dst = reserve(bytes.size());
    std::memcpy(dst, bytes.data(), bytes.size());
    return {dst, bytes.size()};
}

Router::Router(Venue venue, Metrics& metrics) : venue_(venue), metrics_(metrics) {
    // allocate the parser's buffers up front so the first messages don't have to
    if (parser_.allocate(1 << 20) != simdjson::SUCCESS) logMsg(LogLevel::Warn, "simdjson: couldn't preallocate");
    frame_.reserve(64 * 1024);
    minified_.reserve(64 * 1024);
}

void Router::add(SymbolPipeline& pipeline) { routes_[pipeline.streamName()] = &pipeline; }

bool Router::resolve(std::string_view stream, SymbolPipeline*& pipeline, StreamKind& kind) const {
    // "btcusdt@depth5@100ms" -> symbol part + stream type
    size_t at = stream.find('@');
    if (at == std::string_view::npos) return false;
    auto it = routes_.find(stream.substr(0, at));
    if (it == routes_.end()) return false;
    std::string_view type = stream.substr(at + 1);
    if (type == "depth@100ms" || type == "depth") kind = StreamKind::DepthDiff;
    else if (type == "depth5@100ms" || type == "depth5") kind = StreamKind::Depth5;
    else if (type == "trade") kind = StreamKind::Trade;
    else return false;
    pipeline = it->second;
    return true;
}

void Router::badFrame(const char* why, std::string_view frame) {
    metrics_.envelopeErrors++;
    if (metrics_.envelopeErrors <= 20) {
        int shown = static_cast<int>(std::min<size_t>(frame.size(), 160));
        logMsg(LogLevel::Warn, "bad frame (%s): %.*s", why, shown, frame.data());
    }
}

void Router::onMessage(uint32_t shardId, uint64_t connEpoch, uint64_t connSeq, int64_t recvNs,
                       std::string_view frame) {
    auto started = std::chrono::steady_clock::now();
    metrics_.messages++;
    metrics_.bytes += frame.size();

    // beast's buffer doesn't promise the padding simdjson wants, so copy (~2KB, cheap)
    std::string_view input = frame_.assign(frame);

    ondemand::document doc;
    ondemand::object envelope, data;
    std::string_view stream, payload;
    auto padded = simdjson::padded_string_view(input.data(), input.size(), frame_.capacity());
    if (parser_.iterate(padded).get(doc) || doc.get_object().get(envelope))
        return badFrame("not a json object", frame);
    if (envelope.find_field_unordered("stream").get_string().get(stream))
        return badFrame("no stream", frame);
    if (envelope.find_field_unordered("data").get_object().get(data) || data.raw_json().get(payload))
        return badFrame("no data object", frame);
    bool nonEmpty = false;
    if (data.reset().get(nonEmpty)) return badFrame("can't rewind data", frame);

    SymbolPipeline* pipeline = nullptr;
    StreamKind kind{};
    if (!resolve(stream, pipeline, kind)) {
        metrics_.unknownStreams++;
        if (metrics_.unknownStreams <= 20)
            logMsg(LogLevel::Warn, "unknown stream '%.*s'", static_cast<int>(stream.size()), stream.data());
        return;
    }

    // binance already sends minified json so this basically never runs, but the
    // csv contract says minified, so make sure
    if (payload.find_first_of(" \t\r\n") != std::string_view::npos) {
        char* dst = minified_.reserve(payload.size());
        size_t len = 0;
        if (simdjson::minify(payload.data(), payload.size(), dst, len) != simdjson::SUCCESS)
            return badFrame("can't minify payload", frame);
        payload = std::string_view(dst, len);
        metrics_.minifiedPayloads++;
    }

    MessageMeta meta;
    meta.recvSec = recvNs / 1'000'000'000;
    meta.recvNsec = static_cast<int32_t>(recvNs % 1'000'000'000);
    meta.venue = venue_;
    meta.kind = kind;
    meta.shardId = shardId;
    meta.connEpoch = connEpoch;
    meta.connSeq = connSeq;
    pipeline->onMessage(meta, payload, data);

    auto took = std::chrono::steady_clock::now() - started;
    metrics_.latency.add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(took).count()));
}

}  // namespace capture
