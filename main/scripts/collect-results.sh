#!/bin/bash
EXPERIMENT_ID=${1:-$(date +%Y%m%d-%H%M%S)}
OUTDIR=analysis/results/$EXPERIMENT_ID
mkdir -p $OUTDIR

# Thu thập metrics từ Prometheus (giả sử đã port-forward)
echo "Fetching Prometheus metrics..."
curl -s 'http://localhost:9090/api/v1/query?query=histogram_quantile(0.95,rate(http_request_duration_seconds_bucket[5m]))' > $OUTDIR/latency_p95.json

# Lấy logs từ các pod quan trọng
kubectl logs -n course-reg-exp deployment/api-gateway --tail=500 > $OUTDIR/api-gateway.log
kubectl logs -n course-reg-exp deployment/rule-engine --tail=500 > $OUTDIR/rule-engine.log

# Copy kết quả phân bổ từ DB (nếu cần)
echo "Collecting allocation data..."
# Giả định có script riêng kết nối ScyllaDB export CSV
python3 analysis/export_allocation.py --out $OUTDIR/allocation.csv

echo "Results saved to $OUTDIR"