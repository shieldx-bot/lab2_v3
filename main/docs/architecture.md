# Kiến trúc hệ thống đăng ký học phần

## Tổng quan
Hệ thống được thiết kế theo kiến trúc microservice, triển khai trên Kubernetes, sử dụng NATS JetStream làm message queue, ScyllaDB làm database chính, Dragonfly làm cache và distributed lock, Cloudflare làm edge cache.

## Các thành phần chính
- **API Gateway**: tiếp nhận yêu cầu HTTP, xác thực, chống lặp (idempotency), đẩy vào NATS.
- **Rule Engine Worker**: kiểm tra điều kiện tiên quyết, ràng buộc cứng.
- **TOPSIS Engine Worker**: tính điểm ưu tiên dựa trên AHP-TOPSIS.
- **Allocation Optimizer**: giải bài toán phân bổ toàn cục, ghi kết quả.
- **Notification Service**: gửi thông báo cho sinh viên.

## Luồng đăng ký
Xem sơ đồ trong file.

## Triển khai
Mọi manifest đều nằm trong `infrastructure/kubernetes/`. Xem `README.md` để cài đặt.