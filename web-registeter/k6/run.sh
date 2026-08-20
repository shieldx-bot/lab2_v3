#!/usr/bin/env bash
# ============================================================
# run.sh -- chạy k6 tiêm tải (TOPSIS-Hybrid) + trích CHỈ số network.
#
# Cách dùng:
#   ./run.sh [PEAK_VU]
#   ví dụ: ./run.sh 750        # đỉnh 750 VU (mặc định)
#          ./run.sh 100        # đỉnh 100 VU để test nhẹ
#
# Env trỏ tới api-service:
#   BASE_URL=http://<host>:<port> ./run.sh 200
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"

PEAK_VU="${1:-750}"
BASE_URL="${BASE_URL:-http://localhost:4000}"

mkdir -p out

echo "==> k6 tiêm tải: đỉnh ${PEAK_VU} VU vào ${BASE_URL}"
k6 run \
  -e BASE_URL="${BASE_URL}" \
  -e PEAK_VU="${PEAK_VU}" \
  --out json=out/k6_raw.json \
  load_test.js

echo "==> Trích chỉ số network -> out/network_metrics.csv"
python3 extract_network.py out/k6_raw.json out/network_metrics.csv
echo "==> Done: out/network_metrics.csv (timestamp, vus, ...network...) + out/k6_raw.json + summary.json"