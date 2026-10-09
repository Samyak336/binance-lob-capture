#include <cstdio>
#include <string>

#include "config.hpp"
#include "live.hpp"
#include "log.hpp"
#include "metrics.hpp"
#include "replay.hpp"
#include "simdjson.h"

using namespace capture;

int main(int argc, char** argv) {
    Config cfg;
    std::string error;
    switch (parseArgs(argc, argv, cfg, error)) {
        case ParseResult::Help: std::fputs(usage().c_str(), stdout); return 0;
        case ParseResult::Version: std::puts("binance_capture 1.0.0"); return 0;
        case ParseResult::Error: std::fprintf(stderr, "error: %s\n\n%s", error.c_str(), usage().c_str()); return 2;
        case ParseResult::Run: break;
    }

    // simdjson picks its SIMD code at compile time (-march), so log which one we got
    logMsg(LogLevel::Info, "simdjson kernel: %s", simdjson::builtin_implementation()->name().c_str());

    Metrics metrics;
    int rc = cfg.replay ? runReplay(cfg, metrics) : runLive(cfg, metrics);
    metrics.print(stderr, cfg.replay ? "replay" : "live");
    return rc;
}
