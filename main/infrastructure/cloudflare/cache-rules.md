# Cloudflare Cache Rules

Áp dụng cho domain `dangky.university.edu.vn` (đã proxy qua Cloudflare).

## Rule 1: Cache API danh sách môn học/lớp
- **Expression**: `(http.request.uri.path matches "^/api/(courses|sections)")`
- **Cache status**: Eligible for cache
- **Edge TTL**: 10 seconds
- **Browser TTL**: 5 seconds

## Rule 2: Cache trạng thái đăng ký
- **Expression**: `(http.request.uri.path matches "^/api/registration/status/")`
- **Cache status**: Eligible for cache
- **Edge TTL**: 5 seconds
- **Browser TTL**: 0 seconds (không cache trình duyệt)

## Rule 3: Bỏ cache cho các API POST
- **Expression**: `(http.request.method eq "POST")`
- **Action**: Bypass cache

Tạo các Page Rule hoặc Cache Rules trong dashboard Cloudflare → Rules → Cache Rules.