#include "replay.hpp"

#include <charconv>
#include <cinttypes>
#include <filesystem>
#include <memory>
#include <string_view>
#include <vector>

#include "csv.hpp"
#include "log.hpp"
#include "pipeline.hpp"
#include "router.hpp"

namespace capture {

namespace fs = std::filesystem;
namespace ondemand = simdjson::ondemand;

// market data csv columns
enum Col { TSEC, TNSEC, VENUE, KIND, SHARD, EPOCH, SEQ, SYMBOL, PAYLOAD, NUM_COLS };

template <typename T>
static bool toNumber(const std::string& s, T& out) {
    auto res = std::from_chars(s.data(), s.data() + s.size(), out);
    return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

static bool isMarketHeader(const std::vector<std::string>& fields, size_t count) {
    std::string joined;
    for (size_t i = 0; i < count; i++) {
        if (i) joined += ',';
        joined += fields[i];
    }
    return joined == MARKET_HEADER;
}

static bool readMeta(const std::vector<std::string>& f, MessageMeta& meta) {
    return toNumber(f[TSEC], meta.recvSec) && toNumber(f[TNSEC], meta.recvNsec) && meta.recvNsec >= 0 &&
           meta.recvNsec < 1'000'000'000 && parseVenue(f[VENUE], meta.venue) && parseStreamKind(f[KIND], meta.kind) &&
           toNumber(f[SHARD], meta.shardId) && toNumber(f[EPOCH], meta.connEpoch) && toNumber(f[SEQ], meta.connSeq);
}

int runReplay(const std::string& inputPath, const std::string& outputDir, Metrics& metrics) {
    CsvReader reader;
    if (!reader.open(inputPath)) {
        logMsg(LogLevel::Error, "%s", reader.error().c_str());
        return 1;
    }
    std::vector<std::string> fields;
    size_t count = 0;
    if (!reader.next(fields, count) || !isMarketHeader(fields, count)) {
        logMsg(LogLevel::Error, "%s: first line isn't the market data header", inputPath.c_str());
        return 1;
    }

    std::error_code ec;
    fs::create_directories(outputDir, ec);
    fs::path outPath = fs::path(outputDir) / (fs::path(inputPath).stem().string() + "_orderbook.csv");
    if (fs::exists(outPath) && fs::equivalent(outPath, inputPath, ec)) {
        logMsg(LogLevel::Error, "output would overwrite the input file");
        return 1;
    }

    std::unique_ptr<SymbolPipeline> pipeline;
    ondemand::parser parser;
    PaddedBuffer payload;
    uint64_t badRows = 0;
    auto badRow = [&](const char* why) {
        badRows++;
        if (badRows <= 20)
            logMsg(LogLevel::Warn, "%s record %" PRIu64 ": %s", inputPath.c_str(), reader.recordNo(), why);
    };

    while (reader.next(fields, count)) {
        if (count == 1 && fields[0].empty()) continue;  // blank line
        MessageMeta meta;
        if (count != NUM_COLS || !readMeta(fields, meta)) {
            badRow("malformed row");
            continue;
        }
        if (!pipeline) {
            pipeline = std::make_unique<SymbolPipeline>(meta.venue, fields[SYMBOL], metrics);
            if (!pipeline->open("", outPath.string())) {
                logMsg(LogLevel::Error, "can't create %s", outPath.string().c_str());
                return 1;
            }
            logMsg(LogLevel::Info, "replaying %s %s -> %s", toString(meta.venue).data(), fields[SYMBOL].c_str(),
                   outPath.string().c_str());
        } else if (meta.venue != pipeline->venue() || fields[SYMBOL] != pipeline->symbol()) {
            badRow("different venue/symbol than the first row (one instrument per file)");
            continue;
        }

        metrics.messages++;
        metrics.bytes += fields[PAYLOAD].size();
        std::string_view json = payload.assign(fields[PAYLOAD]);
        ondemand::document doc;
        ondemand::object obj;
        if (parser.iterate(simdjson::padded_string_view(json.data(), json.size(), payload.capacity())).get(doc) ||
            doc.get_object().get(obj)) {
            badRow("payload_json isn't a json object");
            continue;
        }
        pipeline->process(meta, obj);
    }
    if (!reader.error().empty()) {
        logMsg(LogLevel::Error, "%s: %s", inputPath.c_str(), reader.error().c_str());
        return 1;
    }
    if (!pipeline) {
        logMsg(LogLevel::Error, "%s has no data rows", inputPath.c_str());
        return 1;
    }
    pipeline->close();
    logMsg(LogLevel::Info, "wrote %s (%" PRIu64 " rows, %" PRIu64 " bad input rows)", outPath.string().c_str(),
           metrics.bookRows, badRows);
    return badRows == 0 && metrics.writeErrors == 0 ? 0 : 3;
}

}  // namespace capture
