#pragma once

#include "config.hpp"
#include "metrics.hpp"

namespace capture {

// connects to binance, writes both csv files per symbol, returns after --duration
// or ctrl+c with everything flushed. returns the exit code
int runLive(const Config& cfg, Metrics& metrics);

}  // namespace capture
