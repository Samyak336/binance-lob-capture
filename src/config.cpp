#include "config.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>

namespace capture {

template <typename T>
static bool toNumber(std::string_view s, T& out) {
    auto res = std::from_chars(s.data(), s.data() + s.size(), out);
    return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

std::string usage() {
    return "Usage:\n"
           "  binance_capture --venue spot|usdm --symbols SYM[,SYM...] [options]\n"
           "  binance_capture --replay MARKET_DATA_CSV [--output-dir DIR]\n"
           "\n"
           "Live capture:\n"
           "  --venue spot|usdm          spot or USD-M futures (default spot)\n"
           "  --symbols LIST             comma separated, case doesn't matter (max 200)\n"
           "  --output-dir DIR           where the csv files go (default ./output)\n"
           "  --duration SECONDS         stop after this long (default 0 = until Ctrl+C)\n"
           "  --symbols-per-shard N      symbols per websocket connection (default 100, max 341)\n"
           "  --stats-interval SECONDS   print a stats line every N seconds (default 10, 0 = off)\n"
           "  --ws-url URL               use another endpoint, e.g. wss://stream.binance.com:443/stream\n"
           "\n"
           "Replay:\n"
           "  --replay FILE              rebuild the order book from FILE, no network;\n"
           "                             writes DIR/<FILE name>_orderbook.csv\n"
           "\n"
           "  --help, --version\n";
}

bool parseSymbols(std::string_view list, std::vector<std::string>& out, std::string& error) {
    out.clear();
    size_t start = 0;
    while (start <= list.size()) {
        size_t comma = std::min(list.find(',', start), list.size());
        std::string sym(list.substr(start, comma - start));
        start = comma + 1;
        if (sym.empty()) continue;
        for (char& c : sym) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        bool ok = static_cast<int>(sym.size()) <= MAX_SYMBOL_LEN &&
                  std::all_of(sym.begin(), sym.end(), [](char c) { return std::isalnum(static_cast<unsigned char>(c)); });
        if (!ok) {
            error = "invalid symbol '" + sym + "' (expected 1-20 letters/digits)";
            return false;
        }
        if (std::find(out.begin(), out.end(), sym) == out.end()) out.push_back(sym);
    }
    if (out.empty()) {
        error = "no symbols given";
        return false;
    }
    if (static_cast<int>(out.size()) > MAX_SYMBOLS) {
        error = "too many symbols (max " + std::to_string(MAX_SYMBOLS) + ")";
        return false;
    }
    return true;
}

Endpoint defaultEndpoint(Venue venue) {
    if (venue == Venue::Spot) return {"stream.binance.com", "9443", "/stream"};
    return {"fstream.binance.com", "443", "/public/stream"};  // raw @trade lives on /public for usdm
}

bool parseWsUrl(std::string_view url, Endpoint& out, std::string& error) {
    if (url.substr(0, 6) != "wss://") {
        error = "--ws-url has to start with wss:// (no plain-text connections)";
        return false;
    }
    url.remove_prefix(6);
    size_t slash = std::min(url.find('/'), url.size());
    std::string_view hostPort = url.substr(0, slash);
    std::string_view path = url.substr(slash);
    if (size_t q = path.find('?'); q != std::string_view::npos) path = path.substr(0, q);

    size_t colon = hostPort.find(':');
    out.host = std::string(hostPort.substr(0, colon));
    out.port = colon == std::string_view::npos ? "443" : std::string(hostPort.substr(colon + 1));
    out.path = path.empty() ? "/stream" : std::string(path);
    int port = 0;
    if (out.host.empty() || !toNumber(std::string_view(out.port), port) || port <= 0 || port > 65535) {
        error = "invalid --ws-url '" + std::string(url) + "'";
        return false;
    }
    return true;
}

std::string streamTarget(const Endpoint& endpoint, const std::vector<std::string>& symbols) {
    std::string target = endpoint.path + "?streams=";
    bool first = true;
    for (const std::string& sym : symbols) {
        std::string lower = sym;
        for (char& c : lower) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        for (const char* suffix : {"@depth@100ms", "@depth5@100ms", "@trade"}) {
            if (!first) target += '/';
            first = false;
            target += lower;
            target += suffix;
        }
    }
    return target;
}

ParseResult parseArgs(int argc, const char* const* argv, Config& cfg, std::string& error) {
    bool gotSymbols = false;
    for (int i = 1; i < argc; i++) {
        std::string_view arg = argv[i];
        std::string_view inlineValue;
        bool hasInline = false;
        // allow both "--flag value" and "--flag=value"
        if (size_t eq = arg.find('='); arg.substr(0, 2) == "--" && eq != std::string_view::npos) {
            inlineValue = arg.substr(eq + 1);
            arg = arg.substr(0, eq);
            hasInline = true;
        }
        auto value = [&](std::string_view& v) {
            if (hasInline) {
                v = inlineValue;
                return true;
            }
            if (i + 1 >= argc) {
                error = std::string(arg) + " needs a value";
                return false;
            }
            v = argv[++i];
            return true;
        };
        auto bad = [&](const char* msg) {
            if (error.empty()) error = msg;
            return ParseResult::Error;
        };

        std::string_view v;
        if (arg == "--help" || arg == "-h") return ParseResult::Help;
        if (arg == "--version") return ParseResult::Version;

        if (arg == "--venue") {
            if (!value(v) || !parseVenue(v, cfg.venue)) return bad("--venue must be spot or usdm");
        } else if (arg == "--symbols") {
            if (!value(v) || !parseSymbols(v, cfg.symbols, error)) return bad("bad --symbols");
            gotSymbols = true;
        } else if (arg == "--output-dir") {
            if (!value(v)) return bad("");
            cfg.outputDir = std::string(v);
        } else if (arg == "--duration") {
            if (!value(v) || !toNumber(v, cfg.durationSec) || cfg.durationSec < 0)
                return bad("--duration expects whole seconds");
        } else if (arg == "--symbols-per-shard") {
            if (!value(v) || !toNumber(v, cfg.symbolsPerShard) || cfg.symbolsPerShard <= 0 ||
                cfg.symbolsPerShard * STREAMS_PER_SYMBOL > MAX_STREAMS_PER_CONN)
                return bad("--symbols-per-shard must be 1..341");
        } else if (arg == "--stats-interval") {
            if (!value(v) || !toNumber(v, cfg.statsIntervalSec) || cfg.statsIntervalSec < 0)
                return bad("--stats-interval expects whole seconds");
        } else if (arg == "--ws-url") {
            Endpoint check;
            if (!value(v) || !parseWsUrl(v, check, error)) return bad("bad --ws-url");
            cfg.wsUrl = std::string(v);
        } else if (arg == "--replay") {
            if (!value(v)) return bad("");
            cfg.replay = true;
            cfg.replayFile = std::string(v);
        } else {
            error = "unknown argument '" + std::string(arg) + "'";
            return ParseResult::Error;
        }
    }
    if (!cfg.replay && !gotSymbols) {
        error = "--symbols is required for live capture";
        return ParseResult::Error;
    }
    return ParseResult::Run;
}

}  // namespace capture
