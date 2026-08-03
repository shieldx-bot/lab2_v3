-- ============================================
-- TẠO KEYSPACE
-- ============================================
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
    don_gia        decimal,
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
-- 2b. HỒ SƠ HỌC VỤ SINH VIÊN (MỚI)
-- ============================================
-- Nguồn dữ liệu cho 9/10 tiêu chí TOPSIS-Business (C1-C6, C8-C10). Tiêu chí
-- C7 (xung đột thời khóa biểu) chưa wire, luôn tính = 0 ở tầng ứng dụng nên
-- không cần cột riêng ở đây. Xem chi tiết ánh xạ cột <-> tiêu chí trong
-- comment của hàm processPendingBatch() ở db-service.
CREATE TABLE IF NOT EXISTS sinh_vien_ho_so (
    ma_sinh_vien                text,
    uu_tien_bat_buoc             int,  -- C1 (Benefit): mức bắt buộc của học phần (1-5)
    nguy_co_cham_tot_nghiep      int,  -- C2 (Benefit): nguy cơ chậm tốt nghiệp (1-5)
    so_ky_da_cho                 int,  -- C3 (Benefit): số học kỳ đã chờ học phần này
    so_lan_dang_ky_that_bai      int,  -- C4 (Benefit): số lần đăng ký học phần này thất bại trước đó
    so_tin_chi_tich_luy          int,  -- C5 (Benefit): số tín chỉ đã tích lũy
    khoi_luong_hoc_ky_hien_tai   int,  -- C6 (Cost):    tổng số tín chỉ đang đăng ký học kỳ này
    muc_phu_hop_nguyen_vong      int,  -- C8 (Benefit): mức phù hợp với nguyện vọng ưu tiên (1-5)
    so_lop_thay_the              int,  -- C9 (Cost):    số lớp thay thế khả dụng
    kha_nang_mo_them_lop         int,  -- C10 (Cost):   khả năng nhà trường mở thêm lớp (1-5)
    updated_at                  timestamp,
    PRIMARY KEY (ma_sinh_vien)
);

-- ============================================
-- 3. LỚP HỌC PHẦN
-- ============================================
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
-- 5. ĐĂNG KÝ (đã phi chuẩn hóa)
-- ============================================
-- MỚI: trang_thai giờ có thêm 2 giá trị trong vòng đời nguyện vọng:
--   'ChoXuLy'     -- vừa ghi nhận, đang chờ TOPSIS-Batch xử lý (giai đoạn 1)
--   'DaDangKy'    -- đã được xác nhận có chỗ (không đổi so với bản cũ)
--   'DanhSachCho' -- xếp hạng hợp lệ nhưng hết chỗ, vào danh sách chờ
--   'TuChoi'      -- bị loại ở Rule Engine (xem cột ly_do_tu_choi)
-- MỚI: thêm cột ly_do_tu_choi để trả lý do khi trang_thai='TuChoi'.
CREATE TABLE IF NOT EXISTS dang_ky (
    ma_sinh_vien       text,
    ma_lop_hoc_phan    text,
    ma_dang_ky         text,
    ho                 text,
    ten                text,
    ten_lop_hoc_phan   text,
    ma_mon_hoc         text,
    phong_hoc          text,
    thoi_khoa_bieu     text,
    so_luong_toi_da    int,
    hinh_thuc          text,
    ngay_dang_ky       timestamp,
    trang_thai         text,
    ly_do_tu_choi      text,
    created_at         timestamp,
    updated_at         timestamp,
    PRIMARY KEY ((ma_sinh_vien), ma_lop_hoc_phan)
);