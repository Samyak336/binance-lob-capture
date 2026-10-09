#pragma once

#include "config.hpp"
#include "metrics.hpp"

namespace capture {

// Rebuilds the order book from a market data csv written by live mode, no network.
// If <name>_snapshots.csv sits next to it (rest mode), its REST snapshots are put
// back right after the rows they originally followed.
// Output goes to <outputDir>/<input name>_orderbook.csv - same name live mode uses,
// so the two files can be compared with cmp. Returns the exit code.
int runReplay(const Config& cfg, Metrics& metrics);

}  // namespace capture
