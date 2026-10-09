#pragma once

#include <string>

#include "metrics.hpp"

namespace capture {

// Rebuilds the order book from a market data csv written by live mode, no network.
// Output goes to <outputDir>/<input name>_orderbook.csv - same name live mode uses,
// so the two files can be compared with cmp. Returns the exit code.
int runReplay(const std::string& inputPath, const std::string& outputDir, Metrics& metrics);

}  // namespace capture
