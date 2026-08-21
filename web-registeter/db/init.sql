-- ============================================
-- SCHEMA CHO DB-SERVICE (TOPSIS-HYBRID)
-- ============================================
-- Chạy trên ScyllaDB/Cassandra trước khi deploy db-service:
--   cqlsh -f init.sql
--
-- Schema này khớp với code db.go bản HYBRID:
--   - TOPSIS-Business xếp hạng 10 tiêu chí (C1-C10).
--   - AllocationOptimizer phân bổ toàn cục CÓ dùng nguyện vọng phụ
--     (lớp thay thế) -- cột *_phu trên bảng dang_ky.
-- Bảng sinh_vien_ho_so lưu 9/10 tiêu chí; C7 (xung đột thời khóa biểu)
-- được TÍNH từ chuỗi thoi_khoa_bieu (xem countScheduleConflicts trong
-- db.go) nên không cần cột riêng.

CREATE KEYSPACE IF NOT EXISTS my_keyspace
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'replication_factor': 1
};

USE my_keyspace;

-- ============================================
-- 1. MÔN HỌC
-- ============================================
CREATE TABLE IF NOT EXISTS mon_hoc (
    ma_mon_hoc     text,
    ten_mon_hoc    text,
    so_tin_chi     int,
    don_gia        double,
    trang_thai     text,
    created_at     timestamp,
    updated_at     timestamp,
    PRIMARY KEY (ma_mon_hoc)
);

-- ============================================
-- 2. SINH VIÊN
-- ============================================
CREATE TABLE IF NOT EXISTS sinh_vien (
    ma_sinh_vien   text,
    ho             text,
    ten            text,
    gioi_tinh      text,
    email          text,
    so_dien_thoai  text,
    ma_lop         text,
    created_at     timestamp,
    updated_at     timestamp,
    PRIMARY KEY (ma_sinh_vien)
);

-- ============================================
-- 2b. HỒ SƠ HỌC VỤ SINH VIÊN (NGUỒN TIÊU CHÍ TOPSIS)
-- ============================================
-- Ánh xạ cột <-> tiêu chí (thứ tự khớp topsisCriteria trong db.go):
--   C1  uu_tien_bat_buoc            (Benefit) 1-5
--   C2  nguy_co_cham_tot_nghiep     (Benefit) 1-5
--   C3  so_ky_da_cho                (Benefit) 0+
--   C4  so_lan_dang_ky_that_bai     (Benefit) 0+
--   C5  so_tin_chi_tich_luy         (Benefit) 0+
--   C6  khoi_luong_hoc_ky_hien_tai  (Cost)    0+
--   C7  schedule_conflict           (Cost)    TÍNH từ thoi_khoa_bieu (không lưu)
--   C8  muc_phu_hop_nguyen_vong     (Benefit) 1-5
--   C9  so_lop_thay_the             (Cost)    số lớp thay thế khả dụng
--   C10 kha_nang_mo_them_lop        (Cost)    1-5
CREATE TABLE IF NOT EXISTS sinh_vien_ho_so (
    ma_sinh_vien                text,
    uu_tien_bat_buoc            int,
    nguy_co_cham_tot_nghiep     int,
    so_ky_da_cho                int,
    so_lan_dang_ky_that_bai     int,
    so_tin_chi_tich_luy         int,
    khoi_luong_hoc_ky_hien_tai  int,
    muc_phu_hop_nguyen_vong     int,
    so_lop_thay_the             int,
    kha_nang_mo_them_lop        int,
    updated_at                  timestamp,
    PRIMARY KEY (ma_sinh_vien)
);

-- ============================================
-- 3. LỚP HỌC PHẦN
-- ============================================
-- thoi_khoa_bieu: chuỗi các buổi học, định dạng "T2|07:00-09:00;T4|07:00-09:00"
-- (buổi = <ngày>|<giờ bắt đầu>-<giờ kết thúc>, nhiều buổi cách nhau ';').
-- Dùng để: (a) tính C7 xung đột thời khóa biểu, (b) hiển thị lịch cho FE.
CREATE TABLE IF NOT EXISTS lop_hoc_phan (
    ma_lop_hoc_phan   text,
    ma_mon_hoc        text,
    ten_lop_hoc_phan  text,
    ma_sinh_vien      text,
    phong_hoc         text,
    thoi_khoa_bieu    text,
    so_luong_toi_da   int,
    trang_thai        text,
    ngay_bat_dau      timestamp,
    ngay_ket_thuc     timestamp,
    created_at        timestamp,
    updated_at        timestamp,
    PRIMARY KEY (ma_lop_hoc_phan)
);

-- ============================================
-- 4. COUNTER LỚP HỌC PHẦN
-- ============================================
CREATE TABLE IF NOT EXISTS lop_hoc_phan_counter (
    ma_lop_hoc_phan      text,
    so_luong_da_dang_ky  counter,
    PRIMARY KEY (ma_lop_hoc_phan)
);

-- ============================================
-- 5. ĐĂNG KÝ (đã phi chuẩn hóa, có nguyện vọng phụ cho HYBRID)
-- ============================================
-- Vòng đời nguyện vọng (trang_thai):
--   'ChoXuLy'     -- vừa ghi nhận, đang chờ TOPSIS-Batch xử lý (giai đoạn 1)
--   'DaDangKy'    -- đã được xác nhận có chỗ
--   'DanhSachCho' -- xếp hạng hợp lệ nhưng hết chỗ, vào danh sách chờ
--   'TuChoi'      -- bị loại ở Rule Engine (xem cột ly_do_tu_choi)
--
-- Cột nguyện vọng PHỤ (ma_lop_hoc_phan_phu + các cột phi chuẩn hóa *_phu):
-- lớp THAY THẾ của CÙNG môn học. Khi lớp chính hết chỗ, AllocationOptimizer
-- (chế độ HYBRID) có thể xếp sinh viên sang lớp này; khi đó db-service cập
-- nhật lại các cột chính (ma_lop_hoc_phan, ten_lop_hoc_phan, ...) sang lớp
-- phụ đã được xác nhận.
CREATE TABLE IF NOT EXISTS dang_ky (
    ma_sinh_vien           text,
    ma_lop_hoc_phan        text,
    ma_dang_ky             text,
    ho                     text,
    ten                    text,
    ten_lop_hoc_phan       text,
    ma_mon_hoc             text,
    phong_hoc              text,
    thoi_khoa_bieu         text,
    so_luong_toi_da        int,
    hinh_thuc              text,
    ngay_dang_ky           timestamp,
    trang_thai             text,
    ly_do_tu_choi          text,
    ma_lop_hoc_phan_phu    text,
    ten_lop_hoc_phan_phu   text,
    phong_hoc_phu          text,
    thoi_khoa_bieu_phu     text,
    so_luong_toi_da_phu    int,
    created_at             timestamp,
    updated_at             timestamp,
    PRIMARY KEY ((ma_sinh_vien), ma_lop_hoc_phan)
);

-- ============================================
-- 6. SECONDARY INDEXES (cho ALLOW FILTERING queries)
-- ============================================
CREATE INDEX IF NOT EXISTS idx_dangky_trangthai ON dang_ky(trang_thai);
CREATE INDEX IF NOT EXISTS idx_dangky_madangky ON dang_ky(ma_dang_ky);
CREATE INDEX IF NOT EXISTS idx_dangky_mamonhoc ON dang_ky(ma_mon_hoc);
CREATE INDEX IF NOT EXISTS idx_lhp_mamonhoc ON lop_hoc_phan(ma_mon_hoc);