const cassandra = require("cassandra-driver");
require("dotenv").config();

const client = new cassandra.Client({
  contactPoints: ["192.168.24.2"],
  localDataCenter: "datacenter1",
  keyspace: "my_keyspace",
});

const MON_HOC_COUNT = 120;
const SINH_VIEN_COUNT = 5000;
const LOP_HOC_PHAN_COUNT = 500;
const DANG_KY_PER_SV_MIN = 2;
const DANG_KY_PER_SV_MAX = 4;

const topics = [
  "Nhap mon lap trinh",
  "Cau truc du lieu",
  "Giai thuat nap cao",
  "Lap trinh huong doi tuong",
  "Lap trinh web",
  "Lap trinh di dong",
  "Co so du lieu",
  "He quan tri CSDL",
  "SQL nap cao",
  "Mang may tinh",
  "An toan mang",
  "Mang khong day",
  "He dieu hanh",
  "He nhung",
  "Quan tri he thong",
  "Cong nghe phan mem",
  "Kiem thu phan mem",
  "Phan tich thiet ke he thong",
  "Tri tue nhan tao",
  "Hoc may",
  "Xu ly ngon ngu tu nhien",
  "Do hoa may tinh",
  "Thi giac may tinh",
  "Khoa hoc du lieu",
];

const pricesPool = [
  900000, 1000000, 1100000, 1200000, 1300000, 1400000, 1500000, 1600000,
  1800000, 2000000,
];

const hoNames = [
  "Nguyen",
  "Tran",
  "Le",
  "Pham",
  "Hoang",
  "Huynh",
  "Phan",
  "Vu",
  "Vo",
  "Dang",
  "Bui",
  "Do",
  "Ho",
  "Ngo",
  "Duong",
  "Ly",
  "Trinh",
  "Dinh",
  "Lam",
  "Chu",
  "Mai",
  "Thai",
  "Ha",
  "To",
  "Doan",
  "Truong",
  "Tong",
  "Vuong",
  "Nghiem",
  "Dao",
];

const tenMale = [
  "An",
  "Binh",
  "Cuong",
  "Dung",
  "Hieu",
  "Hung",
  "Kien",
  "Minh",
  "Nam",
  "Phong",
  "Quang",
  "Son",
  "Thanh",
  "Tuan",
  "Viet",
  "Xuan",
  "Duc",
  "Tuan Anh",
  "Dang",
  "Khoi",
];

const tenFemale = [
  "Anh",
  "Bich",
  "Chau",
  "Diem",
  "Em",
  "Ha",
  "Huong",
  "Kim",
  "Linh",
  "Mai",
  "Ngoc",
  "Phuong",
  "Quynh",
  "Thu",
  "Thuy",
  "Van",
  "Yen",
  "Tuyet",
  "Trang",
  "Hong",
];

const tkbList = [
  "Thu 2-Kiet 1",
  "Thu 2-Kiet 2",
  "Thu 2-Kiet 3",
  "Thu 2-Kiet 4",
  "Thu 3-Kiet 1",
  "Thu 3-Kiet 2",
  "Thu 3-Kiet 3",
  "Thu 3-Kiet 4",
  "Thu 4-Kiet 1",
  "Thu 4-Kiet 2",
  "Thu 4-Kiet 3",
  "Thu 4-Kiet 4",
  "Thu 5-Kiet 1",
  "Thu 5-Kiet 2",
  "Thu 5-Kiet 3",
  "Thu 5-Kiet 4",
  "Thu 6-Kiet 1",
  "Thu 6-Kiet 2",
  "Thu 6-Kiet 3",
  "Thu 6-Kiet 4",
  "Thu 7-Kiet 1",
  "Thu 7-Kiet 2",
  "Thu 7-Kiet 3",
];

// ============================================
// BUILD DATA
// ============================================
function buildMonHocs() {
  const rows = [];
  for (let i = 1; i <= MON_HOC_COUNT; i++) {
    rows.push({
      ma_mon_hoc: `IT${String(i).padStart(3, "0")}`,
      ten_mon_hoc: `${topics[(i - 1) % 24]} ${Math.floor((i - 1) / 24) + 1}`,
      so_tin_chi: 2 + (i % 2),
      don_gia: cassandra.types.BigDecimal.fromNumber(pricesPool[i % 10]),
      trang_thai: i % 20 === 0 ? "Khoa" : "Mo",
      created_at: new Date(),
      updated_at: new Date(),
    });
  }
  return rows;
}

function buildSinhViens() {
  const rows = [];
  for (let i = 1; i <= SINH_VIEN_COUNT; i++) {
    const isFemale = i % 2 === 0;
    const ten = isFemale ? tenFemale[(i - 1) % 20] : tenMale[(i - 1) % 20];
    rows.push({
      ma_sinh_vien: `SV${String(i).padStart(5, "0")}`,
      ho: hoNames[(i - 1) % 30],
      ten: ten,
      gioi_tinh: isFemale ? "Nữ" : "Nam",
      email: `sv${i}@university.edu.vn`,
      so_dien_thoai: `0909123${String((i - 1) % 10000).padStart(4, "0")}`,
      ma_lop: `CNTT${String(((i - 1) % 20) + 1).padStart(2, "0")}`,
      created_at: new Date(),
      updated_at: new Date(),
    });
  }
  return rows;
}

function buildLopHocPhans(monHocList) {
  const rows = [];
  for (let i = 1; i <= LOP_HOC_PHAN_COUNT; i++) {
    const maMonHoc = monHocList[(i - 1) % monHocList.length];
    // SỬA: Dùng mã hardcode LHP001 -> LHP500 thay vì UUID
    const maLopHocPhan = `LHP${String(i).padStart(3, "0")}`;

    rows.push({
      ma_lop_hoc_phan: maLopHocPhan,
      ma_mon_hoc: maMonHoc,
      ten_lop_hoc_phan: `Lop ${String(i).padStart(3, "0")}`,
      phong_hoc: `Phong ${String(((i - 1) % 15) + 1).padStart(2, "0")}`,
      thoi_khoa_bieu: tkbList[(i - 1) % tkbList.length],
      so_luong_toi_da: 40 + ((i - 1) % 30),
      trang_thai: "Mo",
      ngay_bat_dau: new Date(2025, 1, ((i - 1) % 30) + 1),
      ngay_ket_thuc: new Date(2025, 5, 30),
      created_at: new Date(),
      updated_at: new Date(),
    });
  }
  return rows;
}

function buildDangKys(sinhViens, lopHocPhans) {
  const rows = [];
  const seen = new Set();
  const today = new Date();

  for (const sv of sinhViens) {
    const count =
      DANG_KY_PER_SV_MIN +
      Math.floor(Math.random() * (DANG_KY_PER_SV_MAX - DANG_KY_PER_SV_MIN + 1));
    let added = 0;
    let attempts = 0;

    while (added < count && attempts < count * 5) {
      attempts++;
      const lhp = lopHocPhans[Math.floor(Math.random() * lopHocPhans.length)];
      const key = `${sv.ma_sinh_vien}-${lhp.ma_lop_hoc_phan}`;
      if (seen.has(key)) continue;
      seen.add(key);

      // SỬA: Dùng mã hardcode cho ma_dang_ky
      const maDangKy = `DK${sv.ma_sinh_vien}_${lhp.ma_lop_hoc_phan}`;

      rows.push({
        ma_dang_ky: maDangKy,
        ma_sinh_vien: sv.ma_sinh_vien,
        ma_lop_hoc_phan: lhp.ma_lop_hoc_phan,
        ho: sv.ho,
        ten: sv.ten,
        ten_lop_hoc_phan: lhp.ten_lop_hoc_phan,
        ma_mon_hoc: lhp.ma_mon_hoc,
        phong_hoc: lhp.phong_hoc,
        thoi_khoa_bieu: lhp.thoi_khoa_bieu,
        so_luong_toi_da: lhp.so_luong_toi_da,
        hinh_thuc: "Chinh quy",
        ngay_dang_ky: new Date(
          today.getTime() -
            Math.floor(Math.random() * 30 * 24 * 60 * 60 * 1000),
        ),
        trang_thai: "DaDangKy",
        created_at: new Date(),
        updated_at: new Date(),
      });
      added++;
    }
  }
  return rows;
}

// ============================================
// INSERT FUNCTIONS
// ============================================
async function insertMonHocs(monHocs) {
  const query = `INSERT INTO mon_hoc (ma_mon_hoc, ten_mon_hoc, so_tin_chi, don_gia, trang_thai, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`;

  const result = [];
  for (const mh of monHocs) {
    await client.execute(
      query,
      [
        mh.ma_mon_hoc,
        mh.ten_mon_hoc,
        mh.so_tin_chi,
        mh.don_gia,
        mh.trang_thai,
        mh.created_at,
        mh.updated_at,
      ],
      { prepare: true },
    );
    result.push(mh.ma_mon_hoc);
  }
  return result;
}

async function insertSinhViens(sinhViens) {
  const query = `INSERT INTO sinh_vien (ma_sinh_vien, ho, ten, gioi_tinh, email, so_dien_thoai, ma_lop, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  const BATCH_SIZE = 100;
  for (let i = 0; i < sinhViens.length; i += BATCH_SIZE) {
    const batch = sinhViens.slice(i, i + BATCH_SIZE).map((sv) => ({
      query,
      params: [
        sv.ma_sinh_vien,
        sv.ho,
        sv.ten,
        sv.gioi_tinh,
        sv.email,
        sv.so_dien_thoai,
        sv.ma_lop,
        sv.created_at,
        sv.updated_at,
      ],
    }));
    await client.batch(batch, { prepare: true });
    if (i % 1000 === 0 && i > 0)
      console.log(`   da insert sinh vien ${i}/${sinhViens.length}`);
  }
}

async function insertLopHocPhans(lopHocPhans) {
  const query = `INSERT INTO lop_hoc_phan (ma_lop_hoc_phan, ma_mon_hoc, ten_lop_hoc_phan, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, trang_thai, ngay_bat_dau, ngay_ket_thuc, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  for (const lhp of lopHocPhans) {
    await client.execute(
      query,
      [
        lhp.ma_lop_hoc_phan,
        lhp.ma_mon_hoc,
        lhp.ten_lop_hoc_phan,
        lhp.phong_hoc,
        lhp.thoi_khoa_bieu,
        lhp.so_luong_toi_da,
        lhp.trang_thai,
        lhp.ngay_bat_dau,
        lhp.ngay_ket_thuc,
        lhp.created_at,
        lhp.updated_at,
      ],
      { prepare: true },
    );

    // Khởi tạo counter = 0
    await client.execute(
      "UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky + 0 WHERE ma_lop_hoc_phan = ?",
      [lhp.ma_lop_hoc_phan],
      { prepare: true },
    );
  }

  return lopHocPhans;
}

async function insertDangKys(dangKys) {
  const BATCH_SIZE = 100;
  let totalInserted = 0;

  for (let i = 0; i < dangKys.length; i += BATCH_SIZE) {
    const batch = [];
    const counterUpdates = [];

    for (const dk of dangKys.slice(i, i + BATCH_SIZE)) {
      batch.push({
        query: `INSERT INTO dang_ky (ma_dang_ky, ma_sinh_vien, ma_lop_hoc_phan, ho, ten, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc, ngay_dang_ky, trang_thai, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        params: [
          dk.ma_dang_ky,
          dk.ma_sinh_vien,
          dk.ma_lop_hoc_phan,
          dk.ho,
          dk.ten,
          dk.ten_lop_hoc_phan,
          dk.ma_mon_hoc,
          dk.phong_hoc,
          dk.thoi_khoa_bieu,
          dk.so_luong_toi_da,
          dk.hinh_thuc,
          dk.ngay_dang_ky,
          dk.trang_thai,
          dk.created_at,
          dk.updated_at,
        ],
      });
      counterUpdates.push(dk.ma_lop_hoc_phan);
    }

    await client.batch(batch, { prepare: true });
    totalInserted += batch.length;

    // Cập nhật counter
    for (const maLopHocPhan of counterUpdates) {
      await client.execute(
        "UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky + 1 WHERE ma_lop_hoc_phan = ?",
        [maLopHocPhan],
        { prepare: true },
      );
    }

    if (totalInserted % 1000 === 0)
      console.log(`   da insert dang ky ${totalInserted}/${dangKys.length}`);
  }
}

// ============================================
// XÓA TẤT CẢ DỮ LIỆU CŨ
// ============================================
async function truncateAllTables() {
  console.log("🗑️  Dang xoa TAT CA du lieu cu...");

  const tables = [
    "dang_ky",
    "lop_hoc_phan_counter",
    "lop_hoc_phan",
    "sinh_vien",
    "mon_hoc",
  ];

  for (const table of tables) {
    try {
      await client.execute(`TRUNCATE ${table}`);
      console.log(`   ✅ Da xoa bang ${table}`);
    } catch (err) {
      console.error(`   ❌ Loi xoa bang ${table}:`, err.message);
    }
  }

  console.log("✅ Da xoa toan bo du lieu cu\n");
}

// ============================================
// MAIN - CHẠY SEED
// ============================================
async function run() {
  const start = Date.now();

  try {
    await client.connect();
    console.log("✅ Ket noi ScyllaDB thanh cong\n");

    // 1. XÓA TẤT CẢ DỮ LIỆU CŨ
    await truncateAllTables();

    // 2. TẠO DỮ LIỆU MỚI
    console.log("🏗️  Dang tao du lieu...");
    const monHocs = buildMonHocs();
    const sinhViens = buildSinhViens();
    console.log(
      `   ✅ Da tao ${monHocs.length} mon hoc, ${sinhViens.length} sinh vien`,
    );

    // 3. INSERT MÔN HỌC
    console.log("\n📚 Dang seed mon hoc...");
    const monHocList = await insertMonHocs(monHocs);
    console.log(`✅ Da seed ${monHocList.length} mon hoc`);

    // 4. INSERT SINH VIÊN
    console.log("\n👨‍🎓 Dang seed sinh vien...");
    await insertSinhViens(sinhViens);
    console.log(`✅ Da seed ${sinhViens.length} sinh vien`);

    // 5. TẠO & INSERT LỚP HỌC PHẦN
    console.log("\n🏫 Dang tao va seed lop hoc phan...");
    const lopHocPhans = buildLopHocPhans(monHocList);
    await insertLopHocPhans(lopHocPhans);
    console.log(`✅ Da seed ${lopHocPhans.length} lop hoc phan`);

    // 6. TẠO & INSERT ĐĂNG KÝ
    console.log("\n📝 Dang tao dang ky...");
    const dangKys = buildDangKys(sinhViens, lopHocPhans);
    console.log(`   Da tao ${dangKys.length} dang ky, bat dau insert...`);
    await insertDangKys(dangKys);
    console.log(`✅ Da seed ${dangKys.length} dang ky`);

    // 7. HIỂN THỊ KẾT QUẢ
    const elapsed = Date.now() - start;
    console.log("\n" + "=".repeat(50));
    console.log("🎉 SEED THANH CONG!");
    console.log("=".repeat(50));
    console.log(`⏱️  Thoi gian: ${(elapsed / 1000).toFixed(1)}s`);
    console.log(`📊 Du lieu da seed:`);
    console.log(`   - Mon hoc: ${monHocList.length}`);
    console.log(`   - Sinh vien: ${sinhViens.length}`);
    console.log(`   - Lop hoc phan: ${lopHocPhans.length}`);
    console.log(`   - Dang ky: ${dangKys.length}`);
    console.log("\n📋 Ma lop hoc phan (vi du):");
    console.log(`   LHP001, LHP002, LHP003, ..., LHP500`);
    console.log("\n📋 Ma dang ky (vi du):");
    console.log(`   DKSV00001_LHP001, DKSV00001_LHP002, ...`);
    console.log("=".repeat(50) + "\n");
  } catch (err) {
    console.error("\n❌ Seed that bai:", err);
    process.exit(1);
  } finally {
    await client.shutdown();
  }
}

// Chạy seed
run().catch((err) => {
  console.error(err);
  process.exit(1);
});
