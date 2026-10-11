#include "csv.hpp"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstring>

#include "log.hpp"

namespace capture {

OutBuffer::OutBuffer(size_t initialCapacity) {
    if (initialCapacity > 0) grow(initialCapacity);
}

void OutBuffer::grow(size_t minCapacity) {
    size_t cap = std::max<size_t>(capacity_ * 2, 4096);
    while (cap < minCapacity) cap *= 2;
    auto bigger = std::make_unique<char[]>(cap);
    if (size_ > 0) std::memcpy(bigger.get(), data_.get(), size_);
    data_ = std::move(bigger);
    capacity_ = cap;
}

void OutBuffer::append(std::string_view s) {
    if (s.empty()) return;
    std::memcpy(reserve(s.size()), s.data(), s.size());
    size_ += s.size();
}

void OutBuffer::appendInt(int64_t v) {
    char* p = reserve(20);  // "-9223372036854775808" is 20 chars
    auto res = std::to_chars(p, p + 20, v);
    size_ += static_cast<size_t>(res.ptr - p);
}

void OutBuffer::appendUint(uint64_t v) {
    char* p = reserve(20);
    auto res = std::to_chars(p, p + 20, v);
    size_ += static_cast<size_t>(res.ptr - p);
}

void appendCsvField(OutBuffer& out, std::string_view field) {
    if (field.find_first_of(",\"\r\n") == std::string_view::npos) {
        out.append(field);
        return;
    }
    // worst case every char is a quote and gets doubled, +2 for the outer quotes
    char* start = out.reserve(field.size() * 2 + 2);
    char* w = start;
    *w++ = '"';
    const char* r = field.data();
    const char* end = r + field.size();
    while (r < end) {
        // copy everything up to the next quote in one go
        auto* q = static_cast<const char*>(std::memchr(r, '"', static_cast<size_t>(end - r)));
        const char* stop = q ? q : end;
        std::memcpy(w, r, static_cast<size_t>(stop - r));
        w += stop - r;
        r = stop;
        if (q) {
            *w++ = '"';
            *w++ = '"';
            r++;
        }
    }
    *w++ = '"';
    out.commit(static_cast<size_t>(w - start));
}

bool CsvWriter::open(const std::string& path, std::string_view header) {
    close();
    std::FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) return false;
    std::setvbuf(f, nullptr, _IONBF, 0);  // we already buffer, no point copying twice
    file_.reset(f);
    path_ = path;
    rows_ = 0;
    buf_.clear();
    buf_.append(header);
    buf_.push('\n');
    return true;
}

bool CsvWriter::flush() {
    if (!file_ || buf_.size() == 0) return true;
    std::string_view data = buf_.view();
    size_t written = std::fwrite(data.data(), 1, data.size(), file_.get());
    buf_.clear();
    if (written != data.size()) {
        // say so straight away (a full disk shouldn't wait for the end of the run), then just count
        if (writeErrors_ == 0) logMsg(LogLevel::Error, "writing %s failed: %s", path_.c_str(), std::strerror(errno));
        writeErrors_++;
        return false;
    }
    return true;
}

void CsvWriter::close() {
    if (!file_) return;
    flush();
    file_.reset();
}

bool CsvReader::open(const std::string& path) {
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) {
        error_ = "cannot open " + path + ": " + std::strerror(errno);
        return false;
    }
    file_.reset(f);
    buf_.resize(1 << 20);
    pos_ = end_ = 0;
    recordNo_ = 0;
    error_.clear();
    return true;
}

bool CsvReader::refill() {
    if (!file_) return false;
    // fields are always copied out before we refill, so the whole buffer can be reused
    end_ = std::fread(buf_.data(), 1, buf_.size(), file_.get());
    pos_ = 0;
    if (end_ == 0 && std::ferror(file_.get())) error_ = "read error";
    return end_ > 0;
}

bool CsvReader::fail(const char* what) {
    error_ = "record " + std::to_string(recordNo_) + ": " + what;
    return false;
}

// called right after the opening quote
bool CsvReader::readQuoted(std::string& field, int& nextChar) {
    while (true) {
        if (pos_ == end_ && !refill()) return fail("unterminated quoted field");
        const char* run = buf_.data() + pos_;
        size_t avail = end_ - pos_;
        auto* q = static_cast<const char*>(std::memchr(run, '"', avail));
        if (!q) {
            field.append(run, avail);
            pos_ = end_;
            continue;
        }
        size_t len = static_cast<size_t>(q - run);
        field.append(run, len);
        pos_ += len + 1;
        int c = get();
        if (c == '"') {  // "" inside quotes = one literal quote
            field.push_back('"');
            continue;
        }
        nextChar = c;
        return true;
    }
}

bool CsvReader::next(std::vector<std::string>& fields, size_t& count) {
    count = 0;
    int c = get();
    if (c == END) return false;
    recordNo_++;
    while (true) {
        if (count == fields.size()) fields.emplace_back();
        std::string& field = fields[count++];
        field.clear();
        if (c == '"') {
            if (!readQuoted(field, c)) return false;
        } else {
            while (c != ',' && c != '\n' && c != '\r' && c != END) {
                if (c == '"') return fail("quote inside unquoted field");
                field.push_back(static_cast<char>(c));
                c = get();
            }
        }
        if (c == ',') {
            c = get();
            continue;
        }
        if (c == '\r') {
            c = get();
            if (c != '\n') return fail("CR not followed by LF");
        }
        if (c == '\n' || c == END) return true;
        return fail("unexpected character after closing quote");
    }
}

}  // namespace capture
