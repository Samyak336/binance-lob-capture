#include "metrics.hpp"

#include <algorithm>
#include <cinttypes>

namespace capture {

void LatencyStats::add(uint64_t ns) {
    int bucket = 0;
    for (uint64_t v = ns; v > 1; v >>= 1) bucket++;
    buckets_[static_cast<size_t>(bucket)]++;
    count_++;
    sum_ += ns;
    max_ = std::max(max_, ns);
}

uint64_t LatencyStats::percentile(double q) const {
    if (count_ == 0) return 0;
    auto target = static_cast<uint64_t>(q * static_cast<double>(count_));
    uint64_t seen = 0;
    for (size_t i = 0; i < buckets_.size(); i++) {
        seen += buckets_[i];
        if (seen > target) return i >= 63 ? max_ : (uint64_t{2} << i);
    }
    return max_;
}

void Metrics::print(std::FILE* out, const char* label) const {
    std::fprintf(out,
                 "[%s] transport: connects=%" PRIu64 " disconnects=%" PRIu64 " messages=%" PRIu64
                 " bytes=%" PRIu64 " envelope_errors=%" PRIu64 " unknown_streams=%" PRIu64
                 " minified_payloads=%" PRIu64 "\n",
                 label, connects, disconnects, messages, bytes, envelopeErrors, unknownStreams, minifiedPayloads);
    std::fprintf(out,
                 "[%s] book: diffs_applied=%" PRIu64 " diffs_stale=%" PRIu64 " diffs_buffered=%" PRIu64
                 " buffer_overflows=%" PRIu64 " gaps=%" PRIu64 " epoch_resets=%" PRIu64 " seeds=%" PRIu64
                 " seed_failures=%" PRIu64 " decode_errors=%" PRIu64 "\n",
                 label, diffsApplied, diffsStale, diffsBuffered, bufferOverflows, gaps, epochResets, seeds,
                 seedFailures, decodeErrors);
    std::fprintf(out,
                 "[%s] depth5: checks=%" PRIu64 " stale=%" PRIu64 " superseded=%" PRIu64
                 " levels_mismatched=%" PRIu64 " levels_learned=%" PRIu64 " | trades=%" PRIu64
                 " uncertain_rows=%" PRIu64 "\n",
                 label, depth5Checks, depth5Stale, depth5Superseded, levelsMismatched, levelsLearned, trades,
                 uncertainRows);
    std::fprintf(out, "[%s] output: market_rows=%" PRIu64 " book_rows=%" PRIu64 " write_errors=%" PRIu64 "\n",
                 label, marketRows, bookRows, writeErrors);
    if (latency.count() > 0) {
        std::fprintf(out,
                     "[%s] latency (read->rows, ns): n=%" PRIu64 " mean=%" PRIu64 " p50<=%" PRIu64
                     " p99<=%" PRIu64 " max=%" PRIu64 "\n",
                     label, latency.count(), latency.mean(), latency.percentile(0.50), latency.percentile(0.99),
                     latency.max());
    }
}

}  // namespace capture
