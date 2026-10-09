#pragma once

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace capture {

// growable byte buffer for building csv rows. it never shrinks, so after the
// first few messages formatting a row doesn't allocate anything
class OutBuffer {
public:
    explicit OutBuffer(size_t initialCapacity = 0);

    // pointer to at least n writable bytes, call commit() after writing
    char* reserve(size_t n) {
        if (capacity_ - size_ < n) grow(size_ + n);
        return data_.get() + size_;
    }
    void commit(size_t n) { size_ += n; }

    void push(char c) {
        *reserve(1) = c;
        size_++;
    }
    void append(std::string_view s);
    void appendInt(int64_t v);
    void appendUint(uint64_t v);

    std::string_view view() const { return {data_.get(), size_}; }
    size_t size() const { return size_; }
    void clear() { size_ = 0; }

private:
    void grow(size_t minCapacity);

    std::unique_ptr<char[]> data_;
    size_t size_ = 0;
    size_t capacity_ = 0;
};

// RFC 4180: quote the field if it has a comma, quote or newline, and double any quotes inside
void appendCsvField(OutBuffer& out, std::string_view field);

// csv output file. rows are built in memory and written out in big chunks,
// so the network thread does one write() per few hundred rows instead of one per row
class CsvWriter {
public:
    static constexpr size_t FLUSH_AT = 256 * 1024;

    CsvWriter() = default;
    CsvWriter(const CsvWriter&) = delete;
    CsvWriter& operator=(const CsvWriter&) = delete;
    ~CsvWriter() { close(); }

    bool open(const std::string& path, std::string_view header);  // truncates
    bool isOpen() const { return file_ != nullptr; }
    const std::string& path() const { return path_; }

    OutBuffer& row() { return buf_; }
    void endRow() {
        buf_.push('\n');
        rows_++;
        if (buf_.size() >= FLUSH_AT) flush();
    }

    bool flush();
    void close();

    uint64_t rows() const { return rows_; }
    uint64_t writeErrors() const { return writeErrors_; }

private:
    struct Closer {
        void operator()(std::FILE* f) const { std::fclose(f); }
    };

    std::unique_ptr<std::FILE, Closer> file_;
    OutBuffer buf_{FLUSH_AT + 64 * 1024};
    std::string path_;
    uint64_t rows_ = 0;
    uint64_t writeErrors_ = 0;
};

// streaming csv reader for replay. handles quoted fields (with commas, quotes,
// newlines inside) and LF or CRLF line endings. field strings are reused between rows
class CsvReader {
public:
    CsvReader() = default;
    CsvReader(const CsvReader&) = delete;
    CsvReader& operator=(const CsvReader&) = delete;

    bool open(const std::string& path);

    // reads one record into fields[0..count). false at end of file or on a bad
    // record - check error() to tell which
    bool next(std::vector<std::string>& fields, size_t& count);

    const std::string& error() const { return error_; }
    uint64_t recordNo() const { return recordNo_; }

private:
    static constexpr int END = -1;

    int get() {
        if (pos_ == end_ && !refill()) return END;
        return static_cast<unsigned char>(buf_[pos_++]);
    }
    bool refill();
    bool fail(const char* what);
    bool readQuoted(std::string& field, int& nextChar);

    struct Closer {
        void operator()(std::FILE* f) const { std::fclose(f); }
    };

    std::unique_ptr<std::FILE, Closer> file_;
    std::vector<char> buf_;
    size_t pos_ = 0;
    size_t end_ = 0;
    uint64_t recordNo_ = 0;
    std::string error_;
};

}  // namespace capture
