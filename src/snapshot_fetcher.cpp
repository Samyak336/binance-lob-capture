#include "snapshot_fetcher.hpp"

#include <algorithm>
#include <charconv>
#include <cinttypes>
#include <string_view>

#include "config.hpp"
#include "log.hpp"

namespace capture {

namespace http = beast::http;
using tcp = net::ip::tcp;
using namespace std::chrono_literals;

constexpr auto REQUEST_TIMEOUT = 10s;  // per step (connect, handshake, write, read)
constexpr auto FIRST_RETRY = 1s;
constexpr auto MAX_RETRY = 30s;
constexpr auto RATE_LIMITED_WAIT = 60s;          // 429/418 without a Retry-After header
constexpr size_t MAX_SNAPSHOT_BYTES = 4 << 20;  // 5000 levels of spot is ~350 KB

// binance's weight counter starts over every wall-clock minute
static int64_t currentMinute() {
    using namespace std::chrono;
    return duration_cast<minutes>(system_clock::now().time_since_epoch()).count();
}

template <typename T>
static bool headerNumber(std::string_view s, T& out) {
    auto res = std::from_chars(s.data(), s.data() + s.size(), out);
    return res.ec == std::errc() && res.ptr == s.data() + s.size();
}

SnapshotFetcher::SnapshotFetcher(net::io_context& ioc, ssl::context& tls, Venue venue, int depth, Metrics& metrics)
    : ioc_(ioc),
      tls_(tls),
      resolver_(ioc),
      timer_(ioc),
      depth_(depth),
      weight_(static_cast<uint64_t>(snapshotWeight(venue, depth))),
      // leave the other half for anything else running from this IP
      weightBudget_(venue == Venue::Spot ? 3000 : 1200),
      host_(venue == Venue::Spot ? "api.binance.com" : "fapi.binance.com"),
      path_(venue == Venue::Spot ? "/api/v3/depth" : "/fapi/v1/depth"),
      metrics_(metrics),
      backoff_(FIRST_RETRY) {}

void SnapshotFetcher::request(SymbolPipeline& pipeline) {
    if (stopping_) return;
    queue_.push_back(&pipeline);
    if (!busy_) next();
}

void SnapshotFetcher::stop() {
    stopping_ = true;
    queue_.clear();
    timer_.cancel();
    resolver_.cancel();
    if (stream_) beast::get_lowest_layer(*stream_).close();  // aborts whatever step is in flight
}

void SnapshotFetcher::next() {
    if (stopping_ || queue_.empty()) {
        busy_ = false;
        return;
    }
    busy_ = true;
    std::chrono::milliseconds wait = weightWait();
    if (wait.count() > 0) {
        logMsg(LogLevel::Info, "REST weight at %" PRIu64 " this minute, next snapshot in %lld ms",
               metrics_.restWeightUsed, static_cast<long long>(wait.count()));
        timer_.expires_after(wait);
        timer_.async_wait([self = shared_from_this()](beast::error_code ec) {
            if (!ec && !self->stopping_) self->send();
        });
        return;
    }
    send();
}

void SnapshotFetcher::send() {
    const SymbolPipeline& p = *queue_.front();
    req_ = {};
    req_.version(11);
    req_.method(http::verb::get);
    req_.target(path_ + "?symbol=" + p.symbol() + "&limit=" + std::to_string(depth_));
    req_.set(http::field::host, host_);
    req_.set(http::field::user_agent, USER_AGENT);
    req_.set(http::field::accept, "application/json");
    req_.set(http::field::connection, "close");

    metrics_.snapshotsRequested++;
    sentAt_ = std::chrono::steady_clock::now();
    stream_.emplace(ioc_, tls_);
    buffer_.consume(buffer_.size());
    resolver_.async_resolve(host_, "443", beast::bind_front_handler(&SnapshotFetcher::onResolve, shared_from_this()));
}

void SnapshotFetcher::onResolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) return retry("resolve", ec.message());
    beast::get_lowest_layer(*stream_).expires_after(REQUEST_TIMEOUT);
    beast::get_lowest_layer(*stream_).async_connect(
        results, beast::bind_front_handler(&SnapshotFetcher::onConnect, shared_from_this()));
}

void SnapshotFetcher::onConnect(beast::error_code ec, const tcp::resolver::results_type::endpoint_type&) {
    if (ec) return retry("connect", ec.message());
    if (!setTlsHost(*stream_, host_, ec)) return retry("tls sni", ec.message());
    beast::get_lowest_layer(*stream_).expires_after(REQUEST_TIMEOUT);
    stream_->async_handshake(ssl::stream_base::client,
                             beast::bind_front_handler(&SnapshotFetcher::onHandshake, shared_from_this()));
}

void SnapshotFetcher::onHandshake(beast::error_code ec) {
    if (ec) return retry("tls handshake", ec.message());
    beast::get_lowest_layer(*stream_).expires_after(REQUEST_TIMEOUT);
    http::async_write(*stream_, req_, beast::bind_front_handler(&SnapshotFetcher::onWrite, shared_from_this()));
}

void SnapshotFetcher::onWrite(beast::error_code ec, size_t) {
    if (ec) return retry("write", ec.message());
    res_.emplace();
    res_->body_limit(MAX_SNAPSHOT_BYTES);
    beast::get_lowest_layer(*stream_).expires_after(REQUEST_TIMEOUT);
    http::async_read(*stream_, buffer_, *res_, beast::bind_front_handler(&SnapshotFetcher::onRead, shared_from_this()));
}

void SnapshotFetcher::onRead(beast::error_code ec, size_t) {
    if (ec) return retry("read", ec.message());
    int64_t recvNs = nowNs();
    auto took = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - sentAt_);
    // the whole body is in (content-length), so skipping the TLS close_notify can't cut anything off
    beast::get_lowest_layer(*stream_).close();

    const Response& res = res_->get();
    readWeight(res);
    unsigned status = res.result_int();
    if (status == 429 || status == 418) {  // too many requests / IP banned for a while
        std::chrono::milliseconds wait = RATE_LIMITED_WAIT;
        long long retryAfter = 0;
        if (auto it = res.find(http::field::retry_after);
            it != res.end() && headerNumber(it->value(), retryAfter) && retryAfter > 0)
            wait = std::chrono::seconds(retryAfter);
        return retry("request", "rate limited (http " + std::to_string(status) + ")", wait);
    }
    if (status != 200) return retry("request", "http " + std::to_string(status));

    std::string_view body = res.body();
    std::string_view input = padded_.assign(body);
    simdjson::ondemand::document doc;
    simdjson::ondemand::object obj;
    if (json_.iterate(simdjson::padded_string_view(input.data(), input.size(), padded_.capacity())).get(doc) ||
        doc.get_object().get(obj))
        return retry("parse", "answer isn't a json object");
    if (DecodeError err = decodeRestSnapshot(obj, snapshot_); err != DecodeError::None)
        return retry("decode", toString(err));

    SymbolPipeline* p = queue_.front();
    queue_.pop_front();
    backoff_ = FIRST_RETRY;
    logMsg(LogLevel::Info, "%s REST snapshot: lastUpdateId=%" PRIu64 ", %zu bids, %zu asks, %lld ms (weight %" PRIu64
           " used this minute)", p->symbol().c_str(), snapshot_.lastId, snapshot_.bids.size(), snapshot_.asks.size(),
           static_cast<long long>(took.count()), metrics_.restWeightUsed);
    // can queue another request for the same symbol if this one turns out to be too old
    p->onRestSnapshot(recvNs, depth_, body, snapshot_);
    next();
}

void SnapshotFetcher::retry(const char* what, const std::string& why, std::chrono::milliseconds delay) {
    if (stopping_) return;  // errors after stop() are just the aborted operations
    metrics_.snapshotErrors++;
    if (stream_) beast::get_lowest_layer(*stream_).close();
    if (delay.count() == 0) {
        delay = backoff_;
        backoff_ = std::min<std::chrono::milliseconds>(backoff_ * 2, MAX_RETRY);
    }
    logMsg(LogLevel::Warn, "%s REST snapshot %s failed: %s; retrying in %lld ms", queue_.front()->symbol().c_str(),
           what, why.c_str(), static_cast<long long>(delay.count()));
    timer_.expires_after(delay);
    timer_.async_wait([self = shared_from_this()](beast::error_code ec) {
        if (!ec && !self->stopping_) self->next();
    });
}

void SnapshotFetcher::readWeight(const Response& res) {
    auto it = res.find("x-mbx-used-weight-1m");
    uint64_t used = 0;
    if (it == res.end() || !headerNumber(it->value(), used)) return;
    metrics_.restWeightUsed = used;
    weightMinute_ = currentMinute();
}

std::chrono::milliseconds SnapshotFetcher::weightWait() const {
    using namespace std::chrono;
    if (weightMinute_ != currentMinute() || metrics_.restWeightUsed + weight_ <= weightBudget_) return 0ms;
    auto now = system_clock::now().time_since_epoch();
    auto nextMinute = duration_cast<minutes>(now) + minutes(1);
    return duration_cast<milliseconds>(nextMinute - now) + 1s;  // +1s in case our clock runs a bit ahead
}

}  // namespace capture
