#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/http.hpp>

#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>

#include "metrics.hpp"
#include "net.hpp"
#include "pipeline.hpp"
#include "router.hpp"

namespace capture {

// Gets REST depth snapshots (GET /api/v3/depth, /fapi/v1/depth) for books that need
// one. One request at a time, on the same io_context thread as the websockets, so a
// snapshot always lands between two websocket messages - which is what lets replay
// put it back in exactly the same place.
//
// Binance limits request weight per IP per minute (spot 6000, usdm 2400) and a 5000
// level spot snapshot costs 250, so starting a lot of symbols at once could run into
// it. Every answer carries X-MBX-USED-WEIGHT-1M; once that's past half the limit the
// next request waits for the next minute. A 429/418 waits for its Retry-After.
class SnapshotFetcher : public std::enable_shared_from_this<SnapshotFetcher> {
public:
    SnapshotFetcher(net::io_context& ioc, ssl::context& tls, Venue venue, int depth, Metrics& metrics);

    void request(SymbolPipeline& pipeline);  // queue a snapshot for this symbol
    void stop();

private:
    using Stream = beast::ssl_stream<beast::tcp_stream>;
    using Response = beast::http::response<beast::http::string_body>;

    void next();
    void send();
    void onResolve(beast::error_code ec, net::ip::tcp::resolver::results_type results);
    void onConnect(beast::error_code ec, const net::ip::tcp::resolver::results_type::endpoint_type&);
    void onHandshake(beast::error_code ec);
    void onWrite(beast::error_code ec, size_t);
    void onRead(beast::error_code ec, size_t);
    void retry(const char* what, const std::string& why, std::chrono::milliseconds delay = {});
    void readWeight(const Response& res);
    std::chrono::milliseconds weightWait() const;

    net::io_context& ioc_;
    ssl::context& tls_;
    net::ip::tcp::resolver resolver_;
    net::steady_timer timer_;
    std::optional<Stream> stream_;  // a fresh connection per request
    beast::flat_buffer buffer_;
    beast::http::request<beast::http::empty_body> req_;
    std::optional<beast::http::response_parser<beast::http::string_body>> res_;

    int depth_;
    uint64_t weight_;        // what one request costs
    uint64_t weightBudget_;  // half of binance's per-minute limit
    std::string host_;
    std::string path_;
    Metrics& metrics_;

    std::deque<SymbolPipeline*> queue_;  // front = the one being fetched
    bool busy_ = false;
    bool stopping_ = false;
    std::chrono::milliseconds backoff_;
    std::chrono::steady_clock::time_point sentAt_;
    int64_t weightMinute_ = -1;  // wall-clock minute the last weight header was read in

    simdjson::ondemand::parser json_;
    PaddedBuffer padded_;
    RestSnapshot snapshot_;
};

}  // namespace capture
