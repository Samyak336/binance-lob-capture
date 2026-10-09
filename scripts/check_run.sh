#!/usr/bin/env bash
# Sanity checks for one capture, no network needed:
#   - both csv headers are exactly what the assignment asks for
#   - every order book row has 26 columns
#   - replaying the market data csv gives back the same order book csv, byte for byte
#
# usage: scripts/check_run.sh output/market_data_spot_BTCUSDT_<date>.csv
set -uo pipefail

BIN=${BIN:-./build/binance_capture}
md=${1:?usage: $0 market_data_<venue>_<SYMBOL>_<date>.csv}
ob="${md%.csv}_orderbook.csv"
status=0

check() {  # check "what" command...
    local what=$1
    shift
    if "$@"; then echo "OK    $what"; else echo "FAIL  $what"; status=1; fi
}

md_header='recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json'
ob_header='tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4'

check "market data header" test "$(head -1 "$md")" = "$md_header"
check "order book header" test "$(head -1 "$ob")" = "$ob_header"
check "every order book row has 26 columns" test "$(awk -F',' 'NR > 1 && NF != 26' "$ob" | wc -l)" -eq 0

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
if "$BIN" --replay "$md" --output-dir "$tmp" 2> "$tmp/replay.log"; then
    check "replay gives the same $(basename "$ob")" cmp -s "$ob" "$tmp/$(basename "$ob")"
else
    cat "$tmp/replay.log"
    check "replay ran" false
fi
exit $status
