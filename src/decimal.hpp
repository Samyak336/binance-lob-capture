#pragma once

#include <cstdint>
#include <limits>
#include <string_view>

#include "types.hpp"

namespace capture {

enum class DecimalError : uint8_t { None, Empty, BadChar, TooPrecise, Overflow };

// "62000.10" -> 6200010000000 (units of 1e-8), no doubles involved.
// digits past the 8th decimal are only allowed if they're zero - if a value
// can't be represented exactly we report it instead of rounding it.
constexpr DecimalError parseScaled(std::string_view s, int64_t& out) {
    constexpr uint64_t maxValue = std::numeric_limits<int64_t>::max();
    constexpr uint64_t scale = SCALE;

    if (s.empty()) return DecimalError::Empty;

    uint64_t whole = 0;
    size_t i = 0;
    bool sawDigit = false;
    for (; i < s.size() && s[i] != '.'; i++) {
        unsigned d = static_cast<unsigned>(s[i] - '0');
        if (d > 9) return DecimalError::BadChar;
        whole = whole * 10 + d;
        if (whole > maxValue / scale) return DecimalError::Overflow;  // also keeps whole*10 from wrapping
        sawDigit = true;
    }

    uint64_t frac = 0;
    int fracDigits = 0;
    if (i < s.size()) {
        for (i++; i < s.size(); i++) {  // i++ skips the '.'
            unsigned d = static_cast<unsigned>(s[i] - '0');
            if (d > 9) return DecimalError::BadChar;
            if (fracDigits < SCALE_DIGITS) {
                frac = frac * 10 + d;
                fracDigits++;
            } else if (d != 0) {
                return DecimalError::TooPrecise;
            }
            sawDigit = true;
        }
    }
    if (!sawDigit) return DecimalError::BadChar;

    for (; fracDigits < SCALE_DIGITS; fracDigits++) frac *= 10;

    uint64_t total = whole * scale + frac;
    if (total > maxValue) return DecimalError::Overflow;
    out = static_cast<int64_t>(total);
    return DecimalError::None;
}

}  // namespace capture
