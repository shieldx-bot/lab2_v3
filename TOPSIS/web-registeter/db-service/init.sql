-- ============================================
-- TẠO KEYSPACE
-- ============================================
CREATE KEYSPACE IF NOT EXISTS my_keyspace
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
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
    created_at         timestamp,
    updated_at         timestamp,
    PRIMARY KEY ((ma_sinh_vien), ma_lop_hoc_phan)
);