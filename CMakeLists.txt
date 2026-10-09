cmake_minimum_required(VERSION 3.16)
project(binance_lob_capture VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(BLC_BUILD_TESTS "Build the unit tests" ON)
option(BLC_WERROR "Treat warnings as errors" OFF)
option(BLC_SANITIZE "Build with AddressSanitizer + UBSan" OFF)
# simdjson's on-demand parser picks its SIMD code at compile time. Without an
# -march flag x86-64 silently falls back to the slow scalar version.
# Use -DBLC_MARCH= (empty) for a portable binary.
set(BLC_MARCH "native" CACHE STRING "value for -march= (empty to leave it out)")

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost 1.74 REQUIRED)  # only asio + beast, both header-only

# simdjson, pinned by version + hash. We only compile its two amalgamated files,
# not its own cmake project (SOURCE_SUBDIR points at a folder that doesn't exist).
# Offline: -DFETCHCONTENT_SOURCE_DIR_SIMDJSON=/path/to/simdjson-4.6.11
include(FetchContent)
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()
FetchContent_Declare(simdjson
  URL      https://github.com/simdjson/simdjson/archive/refs/tags/v4.6.11.tar.gz
  URL_HASH SHA256=61d948fc24f0d793829ad658058e7597d064988a89b4607ea02e401a82df98ff
  SOURCE_SUBDIR amalgamation-only)
FetchContent_MakeAvailable(simdjson)

# flags every target shares (same -march everywhere, or simdjson breaks)
add_library(blc_options INTERFACE)
target_compile_features(blc_options INTERFACE cxx_std_17)
if(BLC_MARCH)
  target_compile_options(blc_options INTERFACE -march=${BLC_MARCH})
endif()
if(BLC_SANITIZE)
  target_compile_options(blc_options INTERFACE -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
  target_link_options(blc_options INTERFACE -fsanitize=address,undefined)
endif()

set(BLC_WARNINGS -Wall -Wextra -Wpedantic -Wshadow -Wconversion)
if(BLC_WERROR)
  list(APPEND BLC_WARNINGS -Werror)
endif()

add_library(blc_simdjson STATIC ${simdjson_SOURCE_DIR}/singleheader/simdjson.cpp)
target_include_directories(blc_simdjson SYSTEM PUBLIC ${simdjson_SOURCE_DIR}/singleheader)
target_compile_definitions(blc_simdjson PUBLIC SIMDJSON_EXCEPTIONS=0)  # error codes, no exceptions
target_link_libraries(blc_simdjson PUBLIC blc_options)

# everything except the network part, so the tests can link it without boost
add_library(blc_core STATIC
  src/book_sync.cpp
  src/config.cpp
  src/csv.cpp
  src/decoder.cpp
  src/log.cpp
  src/metrics.cpp
  src/order_book.cpp
  src/pipeline.cpp
  src/replay.cpp
  src/router.cpp)
target_include_directories(blc_core PUBLIC src)
target_link_libraries(blc_core PUBLIC blc_simdjson blc_options)
target_compile_options(blc_core PRIVATE ${BLC_WARNINGS})

add_executable(binance_capture src/main.cpp src/live.cpp)
target_link_libraries(binance_capture PRIVATE blc_core Boost::headers OpenSSL::SSL OpenSSL::Crypto Threads::Threads)
target_compile_definitions(binance_capture PRIVATE BOOST_BEAST_USE_STD_STRING_VIEW BOOST_ASIO_NO_DEPRECATED)
target_compile_options(binance_capture PRIVATE ${BLC_WARNINGS})

if(BLC_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
