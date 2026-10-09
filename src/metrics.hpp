#pragma once

#include <array>
#include <cstdint>
#include <cstdio>

namespace capture {

// rough latency histogram: bucket i counts samples in [2^i, 2^(i+1)) ns
class LatencyStats {
public:
    void add(uint64_t ns);
    uint64_t count() const { return count_; }
    uint64_t max() const { return max_; }
    uint64_t mean() const { return count_ ? sum_ / count_ : 0; }
    uint64_t percentile(double q) const;  // upper edge of the bucket holding the q-th sample

private:
    std::array<uint64_t, 64> buckets_{};
    uint64_t count_ = 0;
    uint64_t sum_ = 0;
    uint64_t max_ = 0;
};

// counters for everything that can happen. it's all on one thread so plain ints are enough
struct Metrics {
    // network
    uint64_t connects = 0;
    uint64_t disconnects = 0;
    uint64_t messages = 0;
    uint64_t bytes = 0;
    uint64_t envelopeErrors = 0;
    uint64_t unknownStreams = 0;
    uint64_t minifiedPayloads = 0;

    uint64_t decodeErrors = 0;

    // book
    uint64_t diffsApplied = 0;
    uint64_t diffsStale = 0;
    uint64_t diffsBuffered = 0;
    uint64_t bufferOverflows = 0;
    uint64_t gaps = 0;
    uint64_t epochResets = 0;
    uint64_t seeds = 0;
    uint64_t seedFailures = 0;
    uint64_t depth5Checks = 0;
    uint64_t depth5Stale = 0;
    uint64_t depth5Superseded = 0;
    uint64_t levelsMismatched = 0;
    uint64_t levelsLearned = 0;
    uint64_t trades = 0;
    uint64_t uncertainRows = 0;

    // output
    uint64_t marketRows = 0;
    uint64_t bookRows = 0;
    uint64_t writeErrors = 0;

    LatencyStats latency;  // socket read done -> both csv rows formatted (live only)

    void print(std::FILE* out, const char* label) const;
};

}  // namespace capture
