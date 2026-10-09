#include "replay.hpp"

#include <charconv>
#include <cinttypes>
#include <filesystem>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "csv.hpp"
#include "log.hpp"
#include "pipeline.hpp"
#include "router.hpp"

namespace capture {

namespace fs = std::filesystem;
namespace ondemand = simdjson::ondemand;

// market data csv columns
enum MarketCol { TSEC, TNSEC, VENUE, KIND, SHARD, EPOCH, SEQ, SYMBOL, PAYLOAD, MARKET_COLS };
// snapshots csv columns
enum SnapshotCol { S_TSEC, S_TNSEC, S_VENUE, S_SHARD, S_EPOCH, S_AFTER, S_SYMBOL, S_DEPTH, S_PAYLOAD, SNAPSHOT_COLS };

template <typename T>
static bool toNumber(const std::string& s, T& out) {
    auto res = std::from_chars(s.data(), s.data() + s.size(), out);
    return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

static bool isHeader(const std::vector<std::string>& fields, size_t count, std::string_view header) {
    std::string joined;
    for (size_t i = 0; i < count; i++) {
        if (i) joined += ',';
        joined += fields[i];
    }
    return joined == header;
}

static bool readMeta(const std::vector<std::string>& f, MessageMeta& meta) {
    return toNumber(f[TSEC], meta.recvSec) && toNumber(f[TNSEC], meta.recvNsec) && meta.recvNsec >= 0 &&
           meta.recvNsec < 1'000'000'000 && parseVenue(f[VENUE], meta.venue) && parseStreamKind(f[KIND], meta.kind) &&
           toNumber(f[SHARD], meta.shardId) && toNumber(f[EPOCH], meta.connEpoch) && toNumber(f[SEQ], meta.connSeq);
}

struct SavedSnapshot {
    SnapshotMeta meta;
    Venue venue = Venue::Spot;
    std::string symbol;
    std::string payload;
};

static bool readSnapshotMeta(const std::vector<std::string>& f, SavedSnapshot& s) {
    SnapshotMeta& m = s.meta;
    return toNumber(f[S_TSEC], m.recvSec) && toNumber(f[S_TNSEC], m.recvNsec) && m.recvNsec >= 0 &&
           m.recvNsec < 1'000'000'000 && parseVenue(f[S_VENUE], s.venue) && toNumber(f[S_SHARD], m.shardId) &&
           toNumber(f[S_EPOCH], m.connEpoch) && toNumber(f[S_AFTER], m.afterConnSeq) &&
           toNumber(f[S_DEPTH], m.depthLimit) && m.depthLimit > 0;
}

// does the snapshot belong before the market data row (epoch, seq)?
static bool before(const SnapshotMeta& m, uint64_t epoch, uint64_t seq) {
    return m.connEpoch < epoch || (m.connEpoch == epoch && m.afterConnSeq < seq);
}

// a run only fetches a few snapshots, so the whole file is read up front
static bool loadSnapshots(const std::string& path, std::vector<SavedSnapshot>& out) {
    CsvReader reader;
    if (!reader.open(path)) {
        logMsg(LogLevel::Error, "%s", reader.error().c_str());
        return false;
    }
    std::vector<std::string> fields;
    size_t count = 0;
    if (!reader.next(fields, count) || !isHeader(fields, count, SNAPSHOT_HEADER)) {
        logMsg(LogLevel::Error, "%s: first line isn't the snapshots header", path.c_str());
        return false;
    }
    while (reader.next(fields, count)) {
        if (count == 1 && fields[0].empty()) continue;
        SavedSnapshot s;
        if (count != SNAPSHOT_COLS || !readSnapshotMeta(fields, s)) {
            logMsg(LogLevel::Error, "%s record %" PRIu64 ": malformed row", path.c_str(), reader.recordNo());
            return false;
        }
        // live mode writes them in the order it applied them
        if (!out.empty() && before(s.meta, out.back().meta.connEpoch, out.back().meta.afterConnSeq)) {
            logMsg(LogLevel::Error, "%s record %" PRIu64 ": rows out of order", path.c_str(), reader.recordNo());
            return false;
        }
        s.symbol = fields[S_SYMBOL];
        s.payload = std::move(fields[S_PAYLOAD]);
        out.push_back(std::move(s));
    }
    if (!reader.error().empty()) {
        logMsg(LogLevel::Error, "%s: %s", path.c_str(), reader.error().c_str());
        return false;
    }
    return true;
}

int runReplay(const Config& cfg, Metrics& metrics) {
    const std::string& inputPath = cfg.replayFile;
    CsvReader reader;
    if (!reader.open(inputPath)) {
        logMsg(LogLevel::Error, "%s", reader.error().c_str());
        return 1;
    }
    std::vector<std::string> fields;
    size_t count = 0;
    if (!reader.next(fields, count) || !isHeader(fields, count, MARKET_HEADER)) {
        logMsg(LogLevel::Error, "%s: first line isn't the market data header", inputPath.c_str());
        return 1;
    }

    std::error_code ec;
    fs::create_directories(cfg.outputDir, ec);
    std::string stem = fs::path(inputPath).stem().string();
    fs::path outPath = fs::path(cfg.outputDir) / (stem + "_orderbook.csv");
    fs::path snapshotPath = fs::path(inputPath).parent_path() / (stem + "_snapshots.csv");
    if (fs::exists(outPath) && fs::equivalent(outPath, inputPath, ec)) {
        logMsg(LogLevel::Error, "output would overwrite the input file");
        return 1;
    }

    // same seeding as the live run: a snapshots file means it ran in rest mode
    bool haveSnapshots = fs::exists(snapshotPath);
    SeedMode mode = cfg.seedModeGiven ? cfg.seedMode : haveSnapshots ? SeedMode::Rest : SeedMode::Depth5;
    std::vector<SavedSnapshot> snapshots;
    if (mode == SeedMode::Rest) {
        if (!haveSnapshots) {
            logMsg(LogLevel::Error, "--snapshot rest needs %s", snapshotPath.string().c_str());
            return 1;
        }
        if (!loadSnapshots(snapshotPath.string(), snapshots)) return 1;
        logMsg(LogLevel::Info, "seeding from %zu REST snapshot(s) in %s", snapshots.size(),
               snapshotPath.string().c_str());
    } else {
        logMsg(LogLevel::Info, "seeding from depth5 messages");
    }

    std::unique_ptr<SymbolPipeline> pipeline;
    ondemand::parser parser;
    PaddedBuffer payload;
    RestSnapshot rest;
    size_t nextSnapshot = 0;
    uint64_t badRows = 0;
    auto badRow = [&](const char* why) {
        badRows++;
        if (badRows <= 20)
            logMsg(LogLevel::Warn, "%s record %" PRIu64 ": %s", inputPath.c_str(), reader.recordNo(), why);
    };

    // put back the REST snapshots that were applied before market data row (epoch, seq)
    auto applySnapshots = [&](uint64_t epoch, uint64_t seq, bool all) {
        while (nextSnapshot < snapshots.size() && (all || before(snapshots[nextSnapshot].meta, epoch, seq))) {
            const SavedSnapshot& s = snapshots[nextSnapshot++];
            std::string_view json = payload.assign(s.payload);
            ondemand::document doc;
            ondemand::object obj;
            if (!pipeline || s.venue != pipeline->venue() || s.symbol != pipeline->symbol() ||
                parser.iterate(simdjson::padded_string_view(json.data(), json.size(), payload.capacity())).get(doc) ||
                doc.get_object().get(obj) || decodeRestSnapshot(obj, rest) != DecodeError::None) {
                badRows++;
                logMsg(LogLevel::Warn, "%s: snapshot %zu doesn't fit this file or can't be decoded",
                       snapshotPath.string().c_str(), nextSnapshot);
                continue;
            }
            pipeline->processRestSnapshot(s.meta, rest);
        }
    };

    while (reader.next(fields, count)) {
        if (count == 1 && fields[0].empty()) continue;  // blank line
        MessageMeta meta;
        if (count != MARKET_COLS || !readMeta(fields, meta)) {
            badRow("malformed row");
            continue;
        }
        if (!pipeline) {
            pipeline = std::make_unique<SymbolPipeline>(meta.venue, mode, fields[SYMBOL], metrics);
            if (!pipeline->open("", outPath.string(), "")) {
                logMsg(LogLevel::Error, "can't create %s", outPath.string().c_str());
                return 1;
            }
            logMsg(LogLevel::Info, "replaying %s %s -> %s", toString(meta.venue).data(), fields[SYMBOL].c_str(),
                   outPath.string().c_str());
        } else if (meta.venue != pipeline->venue() || fields[SYMBOL] != pipeline->symbol()) {
            badRow("different venue/symbol than the first row (one instrument per file)");
            continue;
        }
        applySnapshots(meta.connEpoch, meta.connSeq, false);

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
    applySnapshots(0, 0, true);  // any that came after the last row
    pipeline->close();
    logMsg(LogLevel::Info, "wrote %s (%" PRIu64 " rows, %" PRIu64 " bad input rows)", outPath.string().c_str(),
           metrics.bookRows, badRows);
    return badRows == 0 && metrics.writeErrors == 0 ? 0 : 3;
}

}  // namespace capture
