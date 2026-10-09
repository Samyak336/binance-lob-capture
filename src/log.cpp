#include "log.hpp"

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>

namespace capture {

static LogLevel minLevel = LogLevel::Info;

void setLogLevel(LogLevel minimum) { minLevel = minimum; }

void logMsg(LogLevel level, const char* fmt, ...) {
    if (level < minLevel) return;
    using namespace std::chrono;
    auto now = system_clock::now();
    std::time_t secs = system_clock::to_time_t(now);
    auto millis = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
    std::tm tm{};
    gmtime_r(&secs, &tm);
    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%S", &tm);

    const char* tag = level == LogLevel::Info ? "INFO" : level == LogLevel::Warn ? "WARN" : "ERROR";
    std::fprintf(stderr, "%s.%03dZ %-5s ", stamp, static_cast<int>(millis), tag);
    va_list args;
    va_start(args, fmt);
    std::vfprintf(stderr, fmt, args);
    va_end(args);
    std::fputc('\n', stderr);
}

}  // namespace capture
