SET search_path TO public;

-- ================================================
-- CORE SCHEMA
-- ================================================
CREATE TABLE IF NOT EXISTS "MonHoc" (
    "MaMonHoc" VARCHAR(50) PRIMARY KEY,
    "TenMonHoc" VARCHAR(200) NOT NULL,
    "SoTinChi" INTEGER DEFAULT 3,
    "DonGia" NUMERIC(12,2) DEFAULT 1200000,
    "TrangThai" VARCHAR(50) DEFAULT 'Mo',
    "CreatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "UpdatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "LopHocPhan" (
    "MaLopHocPhan" SERIAL PRIMARY KEY,
    "MaMonHoc" VARCHAR(50) NOT NULL,
    "TenLopHocPhan" VARCHAR(200),
    "MaSinhVien" VARCHAR(50),
    "PhongHoc" VARCHAR(100),
    "ThoiKhoaBieu" VARCHAR(200),
    "SoLuongToiDa" INTEGER DEFAULT 60,
    "SoLuongDaDangKy" INTEGER DEFAULT 0,
    "TrangThai" VARCHAR(50) DEFAULT 'Mo',
    "NgayBatDau" DATE DEFAULT '2025-02-01',
    "NgayKetThuc" DATE DEFAULT '2025-06-30',
    "CreatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "UpdatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_lhp_monhoc FOREIGN KEY ("MaMonHoc") REFERENCES "MonHoc"("MaMonHoc")
);

CREATE TABLE IF NOT EXISTS "SinhVien" (
    "MaSinhVien" VARCHAR(50) PRIMARY KEY,
    "Ho" VARCHAR(100),
    "Ten" VARCHAR(100),
    "GioiTinh" VARCHAR(10) DEFAULT 'Nam',
    "Email" VARCHAR(200),
    "SoDienThoai" VARCHAR(20),
    "MaLop" VARCHAR(100),
    "CreatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "UpdatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "DangKy" (
    "MaDangKy" SERIAL PRIMARY KEY,
    "MaSinhVien" VARCHAR(50) NOT NULL,
    "MaLopHocPhan" INTEGER NOT NULL,
    "HinhThuc" VARCHAR(50) DEFAULT 'Chinh quy',
    "NgayDangKy" DATE DEFAULT CURRENT_DATE,
    "TrangThai" VARCHAR(50) DEFAULT 'DaDangKy',
    "CreatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "UpdatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_dangky_sinhvien FOREIGN KEY ("MaSinhVien") REFERENCES "SinhVien"("MaSinhVien"),
    CONSTRAINT fk_dangky_lophocphan FOREIGN KEY ("MaLopHocPhan") REFERENCES "LopHocPhan"("MaLopHocPhan"),
    CONSTRAINT uq_sinhvien_lophocphan UNIQUE ("MaSinhVien", "MaLopHocPhan")
);

-- ================================================
-- INDEXES
-- ================================================
CREATE INDEX IF NOT EXISTS idx_dangky_masv ON "DangKy"("MaSinhVien");
CREATE INDEX IF NOT EXISTS idx_dangky_malhp ON "DangKy"("MaLopHocPhan");
CREATE INDEX IF NOT EXISTS idx_lophocphan_mamh ON "LopHocPhan"("MaMonHoc");
CREATE INDEX IF NOT EXISTS idx_lophocphan_trangthai ON "LopHocPhan"("TrangThai");
CREATE INDEX IF NOT EXISTS idx_lophocphan_malhp ON "LopHocPhan"("MaLopHocPhan");

-- ================================================
-- VIEWS (compatibility with db-service.js)
-- ================================================
CREATE OR REPLACE VIEW "GetDanhSachMonHocPhanDangKy" AS
SELECT
    dk."MaDangKy",
    dk."MaSinhVien",
    sv."Ho",
    sv."Ten",
    lhp."MaLopHocPhan",
    lhp."TenLopHocPhan",
    lhp."MaMonHoc",
    lhp."PhongHoc",
    lhp."ThoiKhoaBieu",
    lhp."SoLuongToiDa",
    lhp."SoLuongDaDangKy",
    dk."HinhThuc",
    dk."NgayDangKy",
    dk."TrangThai"
FROM "DangKy" dk
JOIN "SinhVien" sv ON sv."MaSinhVien" = dk."MaSinhVien"
JOIN "LopHocPhan" lhp ON lhp."MaLopHocPhan" = dk."MaLopHocPhan";

CREATE OR REPLACE VIEW "GetDanhSachLopHocPhan" AS
SELECT
    "MaLopHocPhan",
    "TenLopHocPhan",
    "MaMonHoc",
    "PhongHoc",
    "ThoiKhoaBieu",
    "SoLuongToiDa",
    "SoLuongDaDangKy",
    "TrangThai",
    "NgayBatDau",
    "NgayKetThuc"
    "TenLopHocPhan"
FROM "LopHocPhan";

CREATE OR REPLACE VIEW "getchitietlophocphan" AS
SELECT
    lhp."MaLopHocPhan" AS id,
    lhp."TenLopHocPhan",
    lhp."MaMonHoc",
    lhp."PhongHoc",
    lhp."ThoiKhoaBieu",
    lhp."SoLuongToiDa",
    lhp."SoLuongDaDangKy",
    lhp."TrangThai",
    lhp."NgayBatDau",
    lhp."NgayKetThuc"
FROM "LopHocPhan" lhp;


 