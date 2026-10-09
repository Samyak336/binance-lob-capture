#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "types.hpp"

namespace capture {

// limits so a typo on the command line can't blow up memory or hit binance's caps
constexpr int MAX_SYMBOLS = 200;
constexpr int MAX_SYMBOL_LEN = 20;
constexpr int STREAMS_PER_SYMBOL = 3;
constexpr int MAX_STREAMS_PER_CONN = 1024;  // binance's limit
constexpr size_t MAX_URL_LEN = 8192;

struct Endpoint {
    std::string host;
    std::string port;
    std::string path;  // e.g. "/stream", the ?streams=... part gets added per shard
};

struct Config {
    bool replay = false;
    Venue venue = Venue::Spot;
    std::vector<std::string> symbols;  // upper case, no duplicates, in the order given
    std::string outputDir = "output";
    int durationSec = 0;  // 0 = until ctrl+c
    int symbolsPerShard = 100;
    int statsIntervalSec = 10;
    std::string wsUrl;  // optional endpoint override
    std::string replayFile;
};

enum class ParseResult { Run, Help, Version, Error };

ParseResult parseArgs(int argc, const char* const* argv, Config& cfg, std::string& error);
std::string usage();

// "btcusdt,ETHUSDT" -> {"BTCUSDT", "ETHUSDT"}
bool parseSymbols(std::string_view list, std::vector<std::string>& out, std::string& error);

Endpoint defaultEndpoint(Venue venue);
bool parseWsUrl(std::string_view url, Endpoint& out, std::string& error);  // wss://host[:port]/path
// "/stream?streams=btcusdt@depth@100ms/btcusdt@depth5@100ms/btcusdt@trade/..."
std::string streamTarget(const Endpoint& endpoint, const std::vector<std::string>& symbols);

}  // namespace capture
