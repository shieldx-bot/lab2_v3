# Kế hoạch kiểm thử tổng thể cho Lab2

## Mục tiêu
- [ ] Xác minh từng service hoạt động độc lập theo đúng hợp đồng đầu vào/đầu ra.
- [ ] Xác minh luồng end-to-end giữa API, Redis, NATS, DB và các service phụ trợ.
- [ ] Xác minh manifest Kubernetes, Kind cluster, KEDA và custom scheduler trên môi trường local.
- [ ] Có checklist rõ ràng để theo dõi tiến độ kiểm thử và ghi nhận kết quả thực tế.

## 0. Tiền điều kiện cho môi trường local Kind
- [ ] Cluster `multi-node-demo` được tạo thành công từ `cluster.yaml`.
- [ ] `kubectl get nodes` cho thấy 1 control-plane và 5 worker.
- [ ] KEDA và `metrics-server` đã được cài đặt và ở trạng thái sẵn sàng.
- [ ] Custom scheduler `my-scheduler-2` đang chạy và có thể nhận workload.
- [ ] Các image cần thiết đã được build và load vào Kind nếu chưa push lên registry.
- [ ] Namespace và service account cần thiết cho các manifest đã tồn tại trước khi apply.

## 1. Logic Tests

### API Services (`web-registeter/api-services`)
- [ ] Redis connection khởi tạo thành công với `REDIS_HOST` và `REDIS_PORT` từ env/config map.
- [ ] Khi thiếu env, API có thể fallback về `redis.default.svc.cluster.local:6379`.
- [ ] NATS connection tới `nats-js.default.svc.cluster.local:4222` khởi tạo thành công.
- [ ] `generateCacheKey()` tạo cùng một cache key cho cùng một payload dù thứ tự thuộc tính thay đổi.
- [ ] `POST /DangKyHocPhan/GetChiTietLopHocPhan` trả 400 khi thiếu `idLopHocPhan`.
- [ ] `POST /DangKyHocPhan/GetDanhSachMonHocPhanDangKy` trả 400 khi thiếu `masinhvien`.
- [ ] `POST /DangKyHocPhan/GetDanhSachLopHocPhan` trả 400 khi thiếu `TenMonHoc`.
- [ ] `POST /DangKyHocPhan/DangKyMonHoc` trả 400 khi thiếu `maSinhVien` hoặc `maLopHocPhan`.
- [ ] Cache HIT trả dữ liệu từ Redis và không phát sinh publish sang `db.query`.
- [ ] Cache MISS publish `db.query`, nhận `db.response`, rồi ghi kết quả vào cache.
- [ ] Cache entry có TTL đúng 300 giây và hết hạn theo đúng kỳ vọng 5 phút.
- [ ] Request không nhận phản hồi trong 10 giây phải trả 504 với thông báo `DB service timeout`.
- [ ] Lỗi `redis.get()` hoặc `redis.set()` không làm treo request; API vẫn đi theo nhánh DB.
- [ ] Lỗi publish hoặc mất kết nối NATS được xử lý sạch, không để pending request tồn đọng.
- [ ] `DangKyMonHoc` không đi qua cache để tránh cache hóa dữ liệu ghi.
- [ ] Pending request và timer được dọn dẹp sau khi có phản hồi hợp lệ.

### DB Services (`web-registeter/db-service`)
- [ ] PostgreSQL kết nối thành công tới `metricdb-rw.default.svc.cluster.local:5432` bằng password từ env.
- [ ] `GET_CHI_TIET_LOP_HOC_PHAN` map đúng sang câu SQL truy vấn lớp học phần.
- [ ] `GET_DANH_SACH_MON_HOC_PHAN_DANG_KY` xử lý đúng các filter tùy chọn `dotDangKy` và `hinhThuc`.
- [ ] `GET_DANH_SACH_LOP_HOC_PHAN` xử lý đúng danh sách `TenMonHoc` khi đầu vào là một giá trị hoặc một mảng.
- [ ] `DANG_KY_MON_HOC` ghi thành công một bản ghi đăng ký hợp lệ.
- [ ] Khi insert lỗi, service trả `success: false` và không để lại trạng thái nửa chừng.
- [ ] Khi query lỗi hoặc kết nối lỗi, response vẫn được publish về `db.response` với payload rõ ràng.
- [ ] `queryType` không hợp lệ trả lỗi có thể đọc được.
- [ ] Message malformed từ NATS không làm sập process.
- [ ] Nếu có nhiều bước ghi dữ liệu hơn trong tương lai, transaction phải bảo đảm atomicity cho toàn bộ đăng ký.

### Calculator (`calculator`)
- [ ] Service khởi động và bind đúng cổng runtime.
- [ ] Payload đầu vào hợp lệ được parse đúng.
- [ ] Dữ liệu hoặc metric được publish qua NATS theo subject đã định nghĩa.
- [ ] Lỗi publish hoặc mất NATS được báo rõ và không làm treo tiến trình.
- [ ] Payload sai định dạng hoặc thiếu trường bắt buộc bị từ chối an toàn.

### Export Metrix / Collector (`agent/metrix`)
- [ ] Kết nối Redis thành công với `NODE_ID` và `TIME_DELAY`.
- [ ] Chu kỳ lấy mẫu đọc được CPU, Memory, Disk I/O và Network I/O.
- [ ] Mỗi chu kỳ ghi được key `NODE-<id>` bằng `JSON.SET`.
- [ ] Schema metric đầu ra ổn định để service downstream có thể đọc lại.
- [ ] Thiếu `NODE_ID` hoặc `TIME_DELAY` thì service thoát sớm với thông báo rõ ràng.
- [ ] Lỗi Redis trong lúc ghi metric không làm treo vòng lặp vô hạn.
- [ ] Nếu pipeline có bước forward sang NATS, subject và payload phải khớp schema đã thống nhất.

### AI Agent (`agent/random_forest`)
- [ ] Dataset `cloud_dataset.csv` load được và bước train hoàn tất.
- [ ] Model có accuracy thực tế trên bộ test được ghi lại trong kết quả chạy.
- [ ] Service đọc được các key `NODE-*` từ Redis và parse JSON thành bảng dữ liệu.
- [ ] Dự đoán tạo ra node ứng viên hợp lệ khi có node bình thường.
- [ ] Khi không có node bất thường, nhánh chọn ngẫu nhiên vẫn trả về một node hợp lệ.
- [ ] Key `random_forest_TOP` được cập nhật đúng kết quả cuối cùng.
- [ ] Dữ liệu thiếu hoặc JSON hỏng trong Redis không làm crash vòng xử lý.

## 2. Docker Build Tests
- [ ] Build image `api-services`.
- [ ] Build image `db-service`.
- [ ] Build image `calculator`.
- [ ] Build image `metrix` collector.
- [ ] Build image `random_forest` nếu service này được đóng gói thành container trong `agent/random_forest/scheduler-plugins-random_forest`.

## 3. Kubernetes YAML Tests

### Workload Manifests
- [ ] `api-services` Deployment có `schedulerName: my-scheduler-2`.
- [ ] `api-services` Deployment inject `REDIS_HOST` và `REDIS_PORT`.
- [ ] `api-services` container expose `3000`.
- [ ] `db-service` Deployment có `schedulerName: my-scheduler-2`.
- [ ] `db-service` Deployment inject biến `password`.
- [ ] `db-service` container expose `3000`.
- [ ] `redis` StatefulSet có `schedulerName: my-scheduler-2`.
- [ ] `redis` StatefulSet chạy theo mô hình primary/replica với 2 replica.
- [ ] `redis` container expose `6379`.
- [ ] `nats-js` StatefulSet có `schedulerName: my-scheduler-2`.
- [ ] `nats-js` StatefulSet chạy 3 replica.
- [ ] `nats-js` StatefulSet expose client port `4222` và cluster port `6222`.
- [ ] PostgreSQL cluster `metricdb` có 3 instances và storage `10Gi`.
- [ ] Manifest của `metrix` collector chứa đúng image, `NODE_ID`, `URL` và node placement nếu service này được deploy.

### Service Manifests
- [ ] `api-services` Service tồn tại và expose port `3000`.
- [ ] `metricdb-rw` Service tồn tại và expose port `3000`.
- [ ] `redis` Service là headless (`clusterIP: None`) và expose `6379`.
- [ ] `nats-js` Service expose `4222`.
- [ ] `nats-js-headless` Service là headless và expose `4222`/`6222`.

### KEDA ScaledObject
- [ ] `api-services` ScaledObject có `minReplicaCount: 5`.
- [ ] `api-services` ScaledObject có `maxReplicaCount: 30`.
- [ ] `api-services` ScaledObject có CPU trigger `100m`.
- [ ] `db-service` ScaledObject có `minReplicaCount: 5`.
- [ ] `db-service` ScaledObject có `maxReplicaCount: 30`.
- [ ] `db-service` ScaledObject có CPU trigger `100m`.
- [ ] `redis` ScaledObject có `minReplicaCount: 2`.
- [ ] `redis` ScaledObject có `maxReplicaCount: 10`.
- [ ] `redis` ScaledObject có CPU trigger `100m`.
- [ ] Chính sách scale-up và scale-down khớp với yêu cầu ổn định 5 phút.

## 4. Integration Tests
- [ ] Luồng end-to-end client -> API -> Redis cache -> NATS -> DB -> response chạy đúng.
- [ ] Cache HIT trả response trực tiếp từ Redis, không sinh truy vấn DB mới.
- [ ] Cache MISS gọi DB, nhận response, rồi ghi lại vào cache.
- [ ] Redis down thì API vẫn đi theo nhánh fallback và không treo request.
- [ ] NATS down thì API trả timeout hoặc lỗi rõ ràng trong tối đa 10 giây.
- [ ] DB down thì API trả timeout hoặc lỗi rõ ràng trong tối đa 10 giây.
- [ ] `export_metrix` hoặc collector đọc metric từ Redis và đẩy ra pipeline downstream như thiết kế.
- [ ] `random_forest` đọc metric từ Redis và cập nhật node lựa chọn mà không làm hỏng data hiện có.
- [ ] NATS JetStream 3-node vẫn hoạt động sau khi restart một node leader hoặc replica.
- [ ] Redis primary/replica failover vẫn giữ được khả năng đọc và ghi.
- [ ] PostgreSQL cluster failover vẫn giữ được truy vấn đăng ký môn học.
- [ ] Nhiều client concurrent không tạo bản ghi đăng ký trùng lặp ngoài ý muốn.

## 5. Infrastructure Tests
- [ ] Tạo Kind cluster bằng `kind create cluster --name multi-node-demo --config cluster.yaml`.
- [ ] Kiểm tra cụm có đúng 1 control-plane và 5 worker.
- [ ] Điều chỉnh tài nguyên cho từng node bằng `docker update`.
- [ ] Cài đặt KEDA và xác minh các pod/webhook/operator đều `Ready`.
- [ ] Custom scheduler `my-scheduler-2` đang hoạt động và các workload đã dùng nó.
- [ ] NATS JetStream 3-node cluster khởi tạo đủ pod và headless DNS phân giải được.
- [ ] Redis/Dragonfly master-replica cluster khởi tạo đủ pod và replica trỏ đúng primary.
- [ ] PostgreSQL `metricdb` cluster khởi tạo đủ 3 instance.
- [ ] Các image local đã được load vào Kind trước khi áp manifest.
- [ ] `metrics-server` hoạt động để CPU-based scaling chạy được.

## 6. Performance Tests
- [ ] Chạy tải đồng thời mô phỏng đầu kỳ đăng ký môn học với nhiều client.
- [ ] Cache hit rate duy trì trên 80% sau giai đoạn warm-up.
- [ ] Average response time dưới 500ms cho request đọc khi cache đã nóng.
- [ ] API scale từ 5 đến 30 replicas khi CPU vượt `100m`.
- [ ] Scale-up hoàn thành trong dưới 2 phút.
- [ ] Scale-down ổn định sau 5 phút mà không dao động liên tục.
- [ ] Tỷ lệ lỗi 5xx và timeout không tăng bất thường dưới tải cao.
- [ ] Hệ thống chịu được 100+ request đồng thời mà vẫn giữ phản hồi hợp lệ.

## 7. Test Commands

### Build Images
```bash
cd web-registeter/api-services && docker build -t shieldxbot/api-services:v0.0.1 .
cd web-registeter/db-service && docker build -t lab2/db-service:latest .
cd calculator && docker build -t calculator:latest .
cd agent/metrix && docker build -t shieldxbot/lab2-metrix:v0.0.7 .
cd agent/random_forest/scheduler-plugins-random_forest && docker build -t random-forest:latest .
```

### Create Kind Cluster and Tune Resources
```bash
kind create cluster --name multi-node-demo --config cluster.yaml

docker update --cpus="4" --memory="8g" --memory-swap="8g" multi-node-demo-control-plane
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker2
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker3
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker4
docker update --cpus="2" --memory="4g" --memory-swap="4g" multi-node-demo-worker5
```

### Load Images into Kind
```bash
kind load docker-image shieldxbot/api-services:v0.0.1 --name multi-node-demo
kind load docker-image lab2/db-service:latest --name multi-node-demo
kind load docker-image calculator:latest --name multi-node-demo
kind load docker-image shieldxbot/lab2-metrix:v0.0.7 --name multi-node-demo
kind load docker-image random-forest:latest --name multi-node-demo
```

### Deploy Manifests
```bash
kubectl apply -f k8s/api-services/
kubectl apply -f k8s/db-services/
kubectl apply -f k8s/redis/
kubectl apply -f k8s/nats/
kubectl apply -f k8s/postgreSQL/postgre.yaml
kubectl apply -f k8s/metrix.yaml
kubectl get pods -A -o wide
kubectl get deploy,sts,svc,scaledobject -A
kubectl get hpa -A
```

### API Smoke Tests
```bash
kubectl port-forward -n api-services deploy/api-services 3000:3000

curl -i -X POST http://localhost:3000/DangKyHocPhan/GetChiTietLopHocPhan \
  -H "Content-Type: application/json" \
  -d '{"idLopHocPhan":"ABC123"}'

curl -i -X POST http://localhost:3000/DangKyHocPhan/GetDanhSachMonHocPhanDangKy \
  -H "Content-Type: application/json" \
  -d '{"masinhvien":"SV001","dotDangKy":"2025A","hinhThuc":"Chinh quy"}'

curl -i -X POST http://localhost:3000/DangKyHocPhan/GetDanhSachLopHocPhan \
  -H "Content-Type: application/json" \
  -d '{"TenMonHoc":"TOAN"}'

curl -i -X POST http://localhost:3000/DangKyHocPhan/DangKyMonHoc \
  -H "Content-Type: application/json" \
  -d '{"maSinhVien":"SV001","maLopHocPhan":"LHP001","dotDangKy":"2025A","hinhThuc":"Chinh quy"}'
```

### Cache and Redis Checks
```bash
kubectl exec -it <redis-pod> -- redis-cli KEYS 'GET_CHI_TIET_LOP_HOC_PHAN:*'
kubectl exec -it <redis-pod> -- redis-cli TTL 'GET_CHI_TIET_LOP_HOC_PHAN:<computed-key>'
kubectl exec -it <redis-pod> -- redis-cli GET random_forest_TOP
```

### NATS and DB Checks
```bash
kubectl get pods -l app=nats-jetstream
kubectl get svc nats-js nats-js-headless
kubectl logs nats-js-0 --tail=100

kubectl port-forward -n default svc/metricdb-rw 3001:3000
kubectl logs deploy/db-service -n default --tail=100
```

### KEDA and Failover Checks
```bash
kubectl describe scaledobject api-services-scaledobj -n api-services
kubectl describe scaledobject db-service-scaledobj -n default
kubectl describe scaledobject redis-scaledobj -n default
kubectl delete pod nats-js-0
kubectl delete pod redis-0
kubectl get pods -w
```

### Load Test Example
```bash
hey -n 2000 -c 100 -m POST \
  -H "Content-Type: application/json" \
  -d '{"idLopHocPhan":"ABC123"}' \
  http://localhost:3000/DangKyHocPhan/GetChiTietLopHocPhan
```

## 8. Acceptance Criteria
- [ ] Tất cả logic tests pass.
- [ ] Tất cả image build tests pass.
- [ ] Tất cả YAML manifest apply được trên Kind mà không lỗi schema.
- [ ] Luồng end-to-end hoạt động ổn định với cache hit và cache miss.
- [ ] Cache hit rate > 80% sau warm-up.
- [ ] Average response time < 500ms trên request đọc nóng cache.
- [ ] Auto-scaling hoạt động đúng vùng 5-30 replicas.
- [ ] Scale-up < 2 phút và scale-down ổn định sau 5 phút.
- [ ] Hệ thống xử lý được 100+ concurrent requests mà không lỗi dai dẳng.
- [ ] Không còn pod/workload nào phụ thuộc vào scheduler mặc định thay vì `my-scheduler-2`.
