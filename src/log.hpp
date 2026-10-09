#pragma once

namespace capture {

enum class LogLevel { Info, Warn, Error };

// printf style line on stderr with a UTC timestamp. only used off the hot path
// (connects, errors, periodic stats)
#if defined(__GNUC__)
__attribute__((format(printf, 2, 3)))
#endif
void logMsg(LogLevel level, const char* fmt, ...);

// lines below this level are dropped (default Info, i.e. everything)
void setLogLevel(LogLevel minimum);

}  // namespace capture
