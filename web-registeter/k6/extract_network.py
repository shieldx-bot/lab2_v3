#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_network.py -- trích CHỈ CÁC CHỈ SỐ NETWORK từ output json của k6.

Cách chạy:
    k6 run --out json=out/k6_raw.json load_test.js
    python3 extract_network.py out/k6_raw.json [network_metrics.csv]

File kết quả (mặc định network_metrics.csv) có 1 dòng/giây, mỗi dòng có
2 "key" CHÍNH đầu tiên:
    timestamp  -- mốc thời gian (epoch giây)
    vus        -- số lượng USER TRUY CẬP HIỆN TẠI (VU đang chạy)
Kế tiếp là các chỉ số network do k6 đo (byte/giây, req/giây, ms/giây).

Các chỉ số network được lấy theo định nghĩa của k6:
    data_received, data_sent, http_reqs, http_req_failed,
    http_req_blocked, http_req_connecting, http_req_tls_handshaking,
    http_req_sending, http_req_waiting, http_req_receiving,
    http_req_duration (avg + p95), iterations
(không lấy các metric phi-network như iteration_duration, checks, ...)

Hỗ trợ 2 định dạng đầu vào:
  - stream dòng JSON như k6 --out json ghi ra;
  - mảng JSON [ {...}, {...} ].
"""
import csv
import json
import statistics
import sys
from datetime import datetime, timezone


NETWORK_METRICS = {
    # metric -> (cách gộp trong 1 giây)
    'data_received': 'sum',       # byte/giây
    'data_sent': 'sum',           # byte/giây
    'http_reqs': 'sum',           # request/giây
    'http_req_failed': 'rate',    # tỉ lệ lỗi 0..1 trong giây
    'http_req_blocked': 'avg',    # ms (trung bình trong giây)
    'http_req_connecting': 'avg',
    'http_req_tls_handshaking': 'avg',
    'http_req_sending': 'avg',
    'http_req_waiting': 'avg',
    'http_req_receiving': 'avg',
    'http_req_duration': 'avg',   # có thêm p95 riêng
    'iterations': 'sum',
}


def parse_iso(ts):
    """ISO8601 (k6 dùng dạng 2026-08-20T12:00:01.123456789Z) -> epoch giây."""
    return datetime.fromisoformat(ts.replace('Z', '+00:00')).replace(tzinfo=timezone.utc).timestamp()


def load_points(path):
    """Đọc file json của k6 -> list (bucket_epoch, metric, value)."""
    with open(path, 'r', encoding='utf-8') as f:
        first = f.read(1)
        f.seek(0)
        if first == '[':
            data = json.load(f)
        else:
            data = [json.loads(line) for line in f if line.strip()]
    points = []
    for ev in data:
        if isinstance(ev, dict) and ev.get('type') == 'Point':
            m = ev.get('metric', '')
            if m not in NETWORK_METRICS and m not in ('vus', 'vus_max'):
                continue  # chỉ giữ metric mạng + số user
            bucket = int(parse_iso(ev['data']['time']) // 1)
            points.append((bucket, m, ev['data']['value']))
    return points


def main():
    if len(sys.argv) < 2:
        print('Dùng: python3 extract_network.py <k6_raw.json> [<output.csv>]')
        sys.exit(1)
    src = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else 'network_metrics.csv'

    points = load_points(src)
    if not points:
        print(f'Không tìm thấy point network nào trong {src}')
        sys.exit(1)

    # bucket giây -> {metric: [values]}
    buckets = {}
    for bucket, metric, value in points:
        buckets.setdefault(bucket, {}).setdefault(metric, []).append(value)

    headers = [
        'timestamp',  # key chính 1
        'vus',        # key chính 2: số user truy cập hiện tại
        'vus_max',
        'data_received_bytes_s',
        'data_sent_bytes_s',
        'http_reqs_per_s',
        'http_req_failed_rate',
        'http_req_duration_avg_ms',
        'http_req_duration_p95_ms',
        'http_req_blocked_avg_ms',
        'http_req_connecting_avg_ms',
        'http_req_tls_handshaking_avg_ms',
        'http_req_sending_avg_ms',
        'http_req_waiting_avg_ms',
        'http_req_receiving_avg_ms',
        'iterations_per_s',
    ]

    rows = []
    last_vus = 0
    for bucket in sorted(buckets):
        vals = buckets[bucket]

        # 2 key chính
        ts = bucket
        if vals.get('vus'):
            last_vus = vals['vus'][-1]
        vus = last_vus
        vus_max = vals['vus_max'][-1] if vals.get('vus_max') else last_vus

        durations = vals.get('http_req_duration') or []
        p95 = sorted(durations)[max(0, int(round(0.95 * len(durations))) - 1)] if durations else 0.0
        failed = vals.get('http_req_failed') or []
        failed_rate = (failed.count(1) / len(failed)) if failed else 0.0

        def agg(name):
            vs = vals.get(name) or []
            if not vs:
                return 0.0
            return sum(vs) if NETWORK_METRICS.get(name) == 'sum' else round(statistics.mean(vs), 3)

        row = [
            ts, vus, vus_max,
            round(agg('data_received'), 1), round(agg('data_sent'), 1),
            agg('http_reqs'), round(failed_rate, 4),
            round(agg('http_req_duration'), 3), round(p95, 3),
            agg('http_req_blocked'), agg('http_req_connecting'),
            agg('http_req_tls_handshaking'), agg('http_req_sending'),
            agg('http_req_waiting'), agg('http_req_receiving'),
            agg('iterations'),
        ]
        rows.append(row)

    with open(out, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f'Trích xong: {len(rows)} dòng (1 dòng/giây) -> {out}')
    print(f'Tiêu đề: {", ".join(headers[:4])}, ...')


if __name__ == '__main__':
    main()