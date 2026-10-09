#pragma once

// bits the websocket shards (live.cpp) and the REST client (snapshot_fetcher.cpp) share

#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>

#include <chrono>
#include <cstdint>
#include <string>

namespace capture {

namespace beast = boost::beast;
namespace net = boost::asio;
namespace ssl = net::ssl;

constexpr const char* USER_AGENT = "binance-lob-capture/1.0";

// wall clock in ns - the receive timestamp in both csv files
inline int64_t nowNs() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

// SNI + check that the certificate really is for this host.
// (SSL_ctrl is what the SSL_set_tlsext_host_name macro expands to, minus its C cast)
inline bool setTlsHost(beast::ssl_stream<beast::tcp_stream>& stream, const std::string& host, beast::error_code& ec) {
    if (!SSL_ctrl(stream.native_handle(), SSL_CTRL_SET_TLSEXT_HOSTNAME, TLSEXT_NAMETYPE_host_name,
                  const_cast<char*>(host.c_str()))) {
        ec.assign(static_cast<int>(::ERR_get_error()), net::error::get_ssl_category());
        return false;
    }
    stream.set_verify_callback(ssl::host_name_verification(host));
    return true;
}

}  // namespace capture
