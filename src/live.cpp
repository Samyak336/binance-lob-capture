#include "live.hpp"

#include <boost/asio/signal_set.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <ctime>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "log.hpp"
#include "pipeline.hpp"
#include "router.hpp"

namespace capture {

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using namespace std::chrono_literals;

constexpr auto CONNECT_TIMEOUT = 10s;
constexpr auto HANDSHAKE_TIMEOUT = 10s;
constexpr auto IDLE_TIMEOUT = 30s;  // depth comes every 100ms, so 30s of nothing = dead connection
constexpr auto CLOSE_TIMEOUT = 2s;
constexpr auto FIRST_BACKOFF = 500ms;
constexpr auto MAX_BACKOFF = 30s;
constexpr auto FLUSH_EVERY = 1s;
constexpr auto SHUTDOWN_GRACE = 5s;
constexpr size_t MAX_MESSAGE_BYTES = 4 << 20;

static int64_t nowNs() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

static std::string utcDate() {
    std::time_t now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    char buf[16];
    std::strftime(buf, sizeof buf, "%Y-%m-%d", &tm);
    return buf;
}

// One websocket connection carrying the streams for a group of symbols.
// Everything runs on the single io_context thread, so nothing here is ever
// touched from two threads. Every async op holds a shared_ptr to the shard,
// which keeps it alive until its last handler has run.
class Shard : public std::enable_shared_from_this<Shard> {
public:
    Shard(net::io_context& ioc, ssl::context& tls, Endpoint endpoint, std::string target, uint32_t id, Router& router,
          Metrics& metrics, std::function<void()> onStopped)
        : ioc_(ioc),
          tls_(tls),
          resolver_(ioc),
          retryTimer_(ioc),
          endpoint_(std::move(endpoint)),
          target_(std::move(target)),
          id_(id),
          router_(router),
          metrics_(metrics),
          onStopped_(std::move(onStopped)) {}

    void start() { connect(); }

    void stop() {
        if (stopping_) return;
        stopping_ = true;
        retryTimer_.cancel();
        resolver_.cancel();
        if (ws_ && ws_->is_open()) {
            // send a close frame and keep reading until binance answers (beast needs
            // that). binance can take a while, so give the close its own short timeout
            websocket::stream_base::timeout t{};
            ws_->get_option(t);
            t.handshake_timeout = CLOSE_TIMEOUT;
            ws_->set_option(t);
            ws_->async_close(websocket::close_code::normal, [self = shared_from_this()](beast::error_code ec) {
                // stream_truncated = binance closed tcp without the tls goodbye, happens all the time
                bool clean = !ec || ec == net::ssl::error::stream_truncated;
                bool timedOut = ec == beast::error::timeout;
                logMsg(LogLevel::Info, "shard %u closed (%s)", self->id_,
                       clean ? "clean" : timedOut ? "no reply to close frame, dropped it" : ec.message().c_str());
                self->stopped();
            });
            return;
        }
        if (ws_) beast::get_lowest_layer(*ws_).close();  // kills a connect/handshake that's still in progress
        stopped();
    }

private:
    using Stream = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

    void connect() {
        // found this one while testing reconnects: if a read dies halfway through a
        // message, the partial bytes stay in the buffer and get glued onto the first
        // message of the next connection. so always start with an empty buffer
        buffer_.consume(buffer_.size());
        ws_.emplace(ioc_, tls_);
        resolver_.async_resolve(endpoint_.host, endpoint_.port, beast::bind_front_handler(&Shard::onResolve, shared_from_this()));
    }

    void onResolve(beast::error_code ec, tcp::resolver::results_type results) {
        if (ec) return fail("resolve", ec);
        beast::get_lowest_layer(*ws_).expires_after(CONNECT_TIMEOUT);
        beast::get_lowest_layer(*ws_).async_connect(results, beast::bind_front_handler(&Shard::onConnect, shared_from_this()));
    }

    void onConnect(beast::error_code ec, const tcp::resolver::results_type::endpoint_type&) {
        if (ec) return fail("connect", ec);
        auto& tlsStream = ws_->next_layer();
        // SNI + check the certificate really is for this host.
        // (this is what the SSL_set_tlsext_host_name macro expands to, minus its C cast)
        if (!SSL_ctrl(tlsStream.native_handle(), SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name,
                      const_cast<char*>(endpoint_.host.c_str()))) {
            ec.assign(static_cast<int>(::ERR_get_error()), net::error::get_ssl_category());
            return fail("tls sni", ec);
        }
        tlsStream.set_verify_callback(ssl::host_name_verification(endpoint_.host));
        beast::get_lowest_layer(*ws_).expires_after(CONNECT_TIMEOUT);
        tlsStream.async_handshake(ssl::stream_base::client, beast::bind_front_handler(&Shard::onTlsHandshake, shared_from_this()));
    }

    void onTlsHandshake(beast::error_code ec) {
        if (ec) return fail("tls handshake", ec);
        beast::get_lowest_layer(*ws_).expires_never();  // websocket layer has its own timeouts from here
        websocket::stream_base::timeout t{};
        t.handshake_timeout = HANDSHAKE_TIMEOUT;
        t.idle_timeout = IDLE_TIMEOUT;
        t.keep_alive_pings = true;  // ping after half the idle time before giving up
        ws_->set_option(t);
        ws_->set_option(websocket::stream_base::decorator(
            [](websocket::request_type& req) { req.set(http::field::user_agent, "binance-lob-capture/1.0"); }));
        ws_->read_message_max(MAX_MESSAGE_BYTES);
        ws_->async_handshake(endpoint_.host + ':' + endpoint_.port, target_,
                             beast::bind_front_handler(&Shard::onWsHandshake, shared_from_this()));
    }

    void onWsHandshake(beast::error_code ec) {
        if (ec) return fail("websocket handshake", ec);
        if (connectedBefore_) epoch_++;
        connectedBefore_ = true;
        seq_ = 0;
        backoff_ = FIRST_BACKOFF;
        metrics_.connects++;
        logMsg(LogLevel::Info, "shard %u connected to %s:%s (conn_epoch=%" PRIu64 ")", id_, endpoint_.host.c_str(),
               endpoint_.port.c_str(), epoch_);
        read();
    }

    void read() { ws_->async_read(buffer_, beast::bind_front_handler(&Shard::onRead, shared_from_this())); }

    void onRead(beast::error_code ec, size_t) {
        if (ec) return fail("read", ec);
        int64_t recvNs = nowNs();  // receive time = the moment the whole message is in
        if (!stopping_) {
            auto bytes = buffer_.cdata();
            router_.onMessage(id_, epoch_, seq_++, recvNs,
                              std::string_view(static_cast<const char*>(bytes.data()), bytes.size()));
        }
        buffer_.consume(buffer_.size());
        read();  // while closing we still read (and ignore) until the close frame ends the loop
    }

    void fail(const char* what, beast::error_code ec) {
        if (stopping_) return;  // errors while shutting down are expected
        metrics_.disconnects++;
        logMsg(LogLevel::Warn, "shard %u %s failed: %s; reconnecting in %lld ms", id_, what, ec.message().c_str(),
               static_cast<long long>(backoff_.count()));
        if (ws_) beast::get_lowest_layer(*ws_).close();
        retryTimer_.expires_after(backoff_);
        retryTimer_.async_wait([self = shared_from_this()](beast::error_code waitEc) {
            if (!waitEc && !self->stopping_) self->connect();
        });
        backoff_ = std::min<std::chrono::milliseconds>(backoff_ * 2, MAX_BACKOFF);
    }

    void stopped() {
        if (onStopped_) std::exchange(onStopped_, nullptr)();
    }

    net::io_context& ioc_;
    ssl::context& tls_;
    tcp::resolver resolver_;
    net::steady_timer retryTimer_;
    std::optional<Stream> ws_;
    beast::flat_buffer buffer_;
    Endpoint endpoint_;
    std::string target_;
    uint32_t id_;
    Router& router_;
    Metrics& metrics_;
    std::function<void()> onStopped_;

    uint64_t epoch_ = 0;
    uint64_t seq_ = 0;
    bool connectedBefore_ = false;
    bool stopping_ = false;
    std::chrono::milliseconds backoff_ = FIRST_BACKOFF;
};

int runLive(const Config& cfg, Metrics& metrics) {
    Endpoint endpoint = defaultEndpoint(cfg.venue);
    if (!cfg.wsUrl.empty()) {
        std::string error;
        if (!parseWsUrl(cfg.wsUrl, endpoint, error)) {
            logMsg(LogLevel::Error, "%s", error.c_str());
            return 2;
        }
    }

    // one market data file + one order book file per symbol
    std::error_code fsErr;
    std::filesystem::create_directories(cfg.outputDir, fsErr);
    std::string date = utcDate();
    std::vector<std::unique_ptr<SymbolPipeline>> pipelines;
    Router router(cfg.venue, metrics);
    for (const std::string& sym : cfg.symbols) {
        auto p = std::make_unique<SymbolPipeline>(cfg.venue, sym, metrics);
        std::string name = "market_data_" + std::string(toString(cfg.venue)) + "_" + sym + "_" + date;
        std::string base = (std::filesystem::path(cfg.outputDir) / name).string();
        if (!p->open(base + ".csv", base + "_orderbook.csv")) {
            logMsg(LogLevel::Error, "can't create output files in %s", cfg.outputDir.c_str());
            return 1;
        }
        logMsg(LogLevel::Info, "%s -> %s, %s", sym.c_str(), p->marketPath().c_str(), p->bookPath().c_str());
        router.add(*p);
        pipelines.push_back(std::move(p));
    }

    net::io_context ioc{1};  // one thread does everything: reads, parsing, book, csv
    ssl::context tls{ssl::context::tls_client};
    tls.set_default_verify_paths();
    tls.set_verify_mode(ssl::verify_peer);

    bool stopping = false;
    size_t runningShards = 0;
    net::signal_set signals(ioc, SIGINT, SIGTERM);
    net::steady_timer durationTimer(ioc), statsTimer(ioc), flushTimer(ioc), graceTimer(ioc);

    std::vector<std::shared_ptr<Shard>> shards;
    auto onShardStopped = [&] {
        if (--runningShards == 0) {
            graceTimer.cancel();
            signals.cancel();
        }
    };
    // split the symbols into groups, one connection each
    int per = cfg.symbolsPerShard;
    for (size_t first = 0; first < cfg.symbols.size(); first += static_cast<size_t>(per)) {
        size_t last = std::min(cfg.symbols.size(), first + static_cast<size_t>(per));
        std::vector<std::string> group(cfg.symbols.begin() + static_cast<long>(first),
                                       cfg.symbols.begin() + static_cast<long>(last));
        std::string target = streamTarget(endpoint, group);
        if (target.size() > MAX_URL_LEN) {
            logMsg(LogLevel::Error, "stream url too long (%zu bytes), lower --symbols-per-shard", target.size());
            return 2;
        }
        auto id = static_cast<uint32_t>(shards.size());
        logMsg(LogLevel::Info, "shard %u: wss://%s:%s%s", id, endpoint.host.c_str(), endpoint.port.c_str(),
               target.c_str());
        shards.push_back(
            std::make_shared<Shard>(ioc, tls, endpoint, std::move(target), id, router, metrics, onShardStopped));
    }
    runningShards = shards.size();

    std::function<void(const char*)> shutdown = [&](const char* why) {
        if (stopping) return;
        stopping = true;
        logMsg(LogLevel::Info, "stopping (%s)", why);
        durationTimer.cancel();
        statsTimer.cancel();
        flushTimer.cancel();
        graceTimer.expires_after(SHUTDOWN_GRACE);
        graceTimer.async_wait([&](beast::error_code ec) {
            if (ec) return;
            logMsg(LogLevel::Warn, "shards didn't close in time, stopping anyway");
            ioc.stop();
        });
        for (auto& shard : shards) shard->stop();
    };

    std::function<void()> waitSignal = [&] {
        signals.async_wait([&](beast::error_code ec, int signo) {
            if (ec) return;
            if (stopping) {
                logMsg(LogLevel::Warn, "second signal, exiting now");
                ioc.stop();
                return;
            }
            // re-arm first: if every shard stops right away, shutdown() cancels this
            // wait and run() can return. (the other order hung forever)
            waitSignal();
            shutdown(signo == SIGINT ? "SIGINT" : "SIGTERM");
        });
    };
    waitSignal();

    if (cfg.durationSec > 0) {
        durationTimer.expires_after(std::chrono::seconds(cfg.durationSec));
        durationTimer.async_wait([&](beast::error_code ec) {
            if (!ec) shutdown("duration elapsed");
        });
    }

    // flush every second so a crash loses at most ~1s of rows
    std::function<void()> scheduleFlush = [&] {
        flushTimer.expires_after(FLUSH_EVERY);
        flushTimer.async_wait([&](beast::error_code ec) {
            if (ec) return;
            for (auto& p : pipelines) p->flush();
            scheduleFlush();
        });
    };
    scheduleFlush();

    std::function<void()> scheduleStats = [&] {
        if (cfg.statsIntervalSec == 0) return;
        statsTimer.expires_after(std::chrono::seconds(cfg.statsIntervalSec));
        statsTimer.async_wait([&](beast::error_code ec) {
            if (ec) return;
            size_t liveBooks = 0;
            for (auto& p : pipelines) liveBooks += p->sync().live() ? 1u : 0u;
            logMsg(LogLevel::Info,
                   "stats: messages=%" PRIu64 " market_rows=%" PRIu64 " book_rows=%" PRIu64 " live_books=%zu/%zu"
                   " gaps=%" PRIu64 " disconnects=%" PRIu64 " depth5_checks=%" PRIu64 " mismatched=%" PRIu64
                   " uncertain_rows=%" PRIu64 " p99<=%" PRIu64 "ns",
                   metrics.messages, metrics.marketRows, metrics.bookRows, liveBooks, pipelines.size(), metrics.gaps,
                   metrics.disconnects, metrics.depth5Checks, metrics.levelsMismatched, metrics.uncertainRows,
                   metrics.latency.percentile(0.99));
            scheduleStats();
        });
    };
    scheduleStats();

    for (auto& shard : shards) shard->start();
    ioc.run();

    for (auto& p : pipelines) p->close();
    for (auto& p : pipelines) logMsg(LogLevel::Info, "closed %s and %s", p->marketPath().c_str(), p->bookPath().c_str());
    return metrics.writeErrors == 0 ? 0 : 3;
}

}  // namespace capture
