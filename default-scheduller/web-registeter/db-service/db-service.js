const { connect, StringCodec } = require("nats");
const cassandra = require("cassandra-driver");
require("dotenv").config();

const sc = StringCodec();
let nc;
const scyllaClients = [];

// ============================================
// SCYLLADB POOL
// ============================================
async function initScyllaPool() {
  const ips = ["192.168.24.2", "192.168.24.3", "192.168.24.6"];

  for (const ip of ips) {
    try {
      const client = new cassandra.Client({
        contactPoints: [ip],
        localDataCenter: "datacenter1",
        keyspace: "my_keyspace",
        socketOptions: {
          connectTimeout: 5000,
          readTimeout: 10000,
        },
      });
      await client.connect();
      scyllaClients.push(client);
      console.log(`✅ ScyllaDB: ${ip}`);
    } catch (err) {
      console.error(`❌ ScyllaDB ${ip}:`, err.message);
    }
  }

  if (scyllaClients.length === 0) {
    throw new Error("Không có kết nối ScyllaDB nào!");
  }
  console.log(`📦 ScyllaDB Pool: ${scyllaClients.length} nodes`);
}

function getRandomClient() {
  return scyllaClients[Math.floor(Math.random() * scyllaClients.length)];
}

// ============================================
// HANDLE QUERY (đầy đủ các case từ code cũ)
// ============================================
async function handleQuery(queryType, params) {
  const client = getRandomClient();

  switch (queryType) {
    case "GET_CHI_TIET_LOP_HOC_PHAN": {
      const { idLopHocPhan } = params;

      const lhpResult = await client.execute(
        `SELECT ma_lop_hoc_phan AS id, ten_lop_hoc_phan AS tenmonhoc,
                so_luong_toi_da AS siso, ma_sinh_vien AS giangvien,
                thoi_khoa_bieu AS lichhoc, phong_hoc AS diadiem
         FROM lop_hoc_phan WHERE ma_lop_hoc_phan = ?`,
        [idLopHocPhan],
        { prepare: true },
      );

      if (lhpResult.rows.length === 0) {
        return { success: false, error: "Không tìm thấy lớp học phần" };
      }

      const counterResult = await client.execute(
        `SELECT so_luong_da_dang_ky AS sosvdadangky
         FROM lop_hoc_phan_counter WHERE ma_lop_hoc_phan = ?`,
        [idLopHocPhan],
        { prepare: true },
      );

      const data = lhpResult.rows.map((row) => ({
        ...row,
        sosvdadangky: counterResult.rows[0]?.sosvdadangky || 0,
      }));

      return { success: true, data };
    }

    case "GET_DANH_SACH_MON_HOC_PHAN_DANG_KY": {
      const { masinhvien, dotDangKy, hinhThuc } = params;

      let query = `SELECT ma_dang_ky, ma_sinh_vien, ho, ten,
                          ma_lop_hoc_phan, ten_lop_hoc_phan, ma_mon_hoc,
                          phong_hoc, thoi_khoa_bieu, so_luong_toi_da,
                          hinh_thuc, ngay_dang_ky, trang_thai
                   FROM dang_ky WHERE ma_sinh_vien = ?`;
      const queryParams = [masinhvien];

      if (hinhThuc) {
        query += ` AND hinh_thuc = ? ALLOW FILTERING`;
        queryParams.push(hinhThuc);
      }

      const result = await client.execute(query, queryParams, {
        prepare: true,
      });
      let data = result.rows;

      if (dotDangKy) {
        data = data.filter((row) => {
          const ngayDK = new Date(row.ngay_dang_ky);
          return ngayDK.toISOString().split("T")[0] === dotDangKy;
        });
      }

      if (data.length === 0) {
        return { success: true, data: [] };
      }

      const maLopHocPhans = data.map((row) => row.ma_lop_hoc_phan);
      const placeholders = maLopHocPhans.map(() => "?").join(", ");

      const countersResult = await client.execute(
        `SELECT ma_lop_hoc_phan, so_luong_da_dang_ky
         FROM lop_hoc_phan_counter
         WHERE ma_lop_hoc_phan IN (${placeholders})`,
        maLopHocPhans,
        { prepare: true },
      );

      const counterMap = new Map();
      for (const row of countersResult.rows) {
        counterMap.set(row.ma_lop_hoc_phan, row.so_luong_da_dang_ky);
      }

      const enrichedData = data.map((row) => ({
        ...row,
        so_luong_da_dang_ky: counterMap.get(row.ma_lop_hoc_phan) || 0,
      }));

      return { success: true, data: enrichedData };
    }

    case "GET_DANH_SACH_LOP_HOC_PHAN": {
      let { TenMonHoc } = params;
      if (!Array.isArray(TenMonHoc)) TenMonHoc = [TenMonHoc];

      const placeholders = TenMonHoc.map(() => "?").join(", ");

      const lhpResult = await client.execute(
        `SELECT ma_lop_hoc_phan, ten_lop_hoc_phan, ma_mon_hoc,
                phong_hoc, thoi_khoa_bieu, so_luong_toi_da,
                trang_thai, ngay_bat_dau, ngay_ket_thuc
         FROM lop_hoc_phan WHERE ma_mon_hoc IN (${placeholders}) ALLOW FILTERING`,
        TenMonHoc,
        { prepare: true },
      );

      if (lhpResult.rows.length === 0) {
        return { success: true, data: [] };
      }

      const maLopHocPhans = lhpResult.rows.map((r) => r.ma_lop_hoc_phan);
      const counterPlaceholders = maLopHocPhans.map(() => "?").join(", ");

      const countersResult = await client.execute(
        `SELECT ma_lop_hoc_phan, so_luong_da_dang_ky
         FROM lop_hoc_phan_counter
         WHERE ma_lop_hoc_phan IN (${counterPlaceholders})`,
        maLopHocPhans,
        { prepare: true },
      );

      const counterMap = new Map();
      for (const row of countersResult.rows) {
        counterMap.set(row.ma_lop_hoc_phan, row.so_luong_da_dang_ky);
      }

      const data = lhpResult.rows.map((row) => ({
        ...row,
        so_luong_da_dang_ky: counterMap.get(row.ma_lop_hoc_phan) || 0,
      }));

      return { success: true, data };
    }

    case "DANG_KY_MON_HOC": {
      const { maSinhVien, maLopHocPhan, dotDangKy, hinhThuc } = params;

      // 1. Kiểm tra sinh viên
      const svResult = await client.execute(
        `SELECT ho, ten FROM sinh_vien WHERE ma_sinh_vien = ?`,
        [maSinhVien],
        { prepare: true },
      );
      if (svResult.rows.length === 0) {
        return { success: false, error: "Không tìm thấy sinh viên" };
      }

      // 2. Kiểm tra lớp học phần
      const lhpResult = await client.execute(
        `SELECT ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da
         FROM lop_hoc_phan WHERE ma_lop_hoc_phan = ?`,
        [maLopHocPhan],
        { prepare: true },
      );
      if (lhpResult.rows.length === 0) {
        return { success: false, error: "Không tìm thấy lớp học phần" };
      }

      // 3. Kiểm tra số lượng
      const counterResult = await client.execute(
        `SELECT so_luong_da_dang_ky FROM lop_hoc_phan_counter WHERE ma_lop_hoc_phan = ?`,
        [maLopHocPhan],
        { prepare: true },
      );
      const soLuongDaDangKy = counterResult.rows[0]?.so_luong_da_dang_ky || 0;
      const soLuongToiDa = lhpResult.rows[0].so_luong_toi_da;

      if (soLuongDaDangKy >= soLuongToiDa) {
        return { success: false, error: "Lớp học phần đã đầy" };
      }

      // 4. Kiểm tra trùng
      const existingResult = await client.execute(
        `SELECT ma_dang_ky FROM dang_ky WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?`,
        [maSinhVien, maLopHocPhan],
        { prepare: true },
      );
      if (existingResult.rows.length > 0) {
        return { success: false, error: "Sinh viên đã đăng ký lớp này" };
      }

      // 5. Thực hiện đăng ký
      const sinhVien = svResult.rows[0];
      const lopHocPhan = lhpResult.rows[0];
      const maDangKy = `DK_${maSinhVien}_${maLopHocPhan}_${Date.now()}`;
      const ngayDangKy = new Date();

      await client.execute(
        `INSERT INTO dang_ky (ma_dang_ky, ma_sinh_vien, ma_lop_hoc_phan, ho, ten, ten_lop_hoc_phan, ma_mon_hoc, phong_hoc, thoi_khoa_bieu, so_luong_toi_da, hinh_thuc, ngay_dang_ky, trang_thai, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          maDangKy,
          maSinhVien,
          maLopHocPhan,
          sinhVien.ho,
          sinhVien.ten,
          lopHocPhan.ten_lop_hoc_phan,
          lopHocPhan.ma_mon_hoc,
          lopHocPhan.phong_hoc,
          lopHocPhan.thoi_khoa_bieu,
          lopHocPhan.so_luong_toi_da,
          hinhThuc || "Chinh quy",
          ngayDangKy,
          "DaDangKy",
          ngayDangKy,
          ngayDangKy,
        ],
        { prepare: true },
      );

      // 6. Cập nhật counter
      await client.execute(
        `UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky + 1 WHERE ma_lop_hoc_phan = ?`,
        [maLopHocPhan],
        { prepare: true },
      );

      return {
        success: true,
        data: {
          ma_dang_ky: maDangKy,
          ma_sinh_vien: maSinhVien,
          ma_lop_hoc_phan: maLopHocPhan,
        },
        message: "Đăng ký thành công",
      };
    }

    case "HUY_DANG_KY": {
      const { maSinhVien, maLopHocPhan } = params;

      // 1. Kiểm tra tồn tại
      const existingResult = await client.execute(
        `SELECT ma_dang_ky FROM dang_ky WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?`,
        [maSinhVien, maLopHocPhan],
        { prepare: true },
      );
      if (existingResult.rows.length === 0) {
        return { success: false, error: "Không tìm thấy đăng ký" };
      }

      // 2. Xóa đăng ký
      await client.execute(
        `DELETE FROM dang_ky WHERE ma_sinh_vien = ? AND ma_lop_hoc_phan = ?`,
        [maSinhVien, maLopHocPhan],
        { prepare: true },
      );

      // 3. Giảm counter
      await client.execute(
        `UPDATE lop_hoc_phan_counter SET so_luong_da_dang_ky = so_luong_da_dang_ky - 1 WHERE ma_lop_hoc_phan = ?`,
        [maLopHocPhan],
        { prepare: true },
      );

      return { success: true, message: "Hủy đăng ký thành công" };
    }

    default:
      return { success: false, error: `Unknown queryType: ${queryType}` };
  }
}

async function handleBatchCounterQuery(params) {
  const { maLopHocPhans } = params;
  if (!Array.isArray(maLopHocPhans) || maLopHocPhans.length === 0) {
    return { success: true, data: {} };
  }

  const placeholders = maLopHocPhans.map(() => "?").join(", ");
  const countersResult = await client.execute(
    `SELECT ma_lop_hoc_phan, so_luong_da_dang_ky
     FROM lop_hoc_phan_counter
     WHERE ma_lop_hoc_phan IN (${placeholders})`,
    maLopHocPhans,
    { prepare: true },
  );

  const counterMap = {};
  for (const row of countersResult.rows) {
    counterMap[row.ma_lop_hoc_phan] = row.so_luong_da_dang_ky;
  }

  return { success: true, data: counterMap };
}

// ============================================
// NATS SUBSCRIBER (dùng queue group + msg.respond)
// ============================================
async function startDBService() {
  await initScyllaPool();

  nc = await connect({
    servers: [
      "nats://192.168.24.6:4222",
      "nats://192.168.24.2:5222",
      "nats://192.168.24.3:6222",
    ],
    timeout: 5000,
    reconnect: true,
    maxReconnectAttempts: -1,
  });
  console.log("✅ NATS connected");

  nc.subscribe("db.query", {
    queue: "db-workers",
    callback: async (err, msg) => {
      if (err) {
        console.error("❌ Lỗi nhận message:", err);
        return;
      }

      let request;
      try {
        request = JSON.parse(sc.decode(msg.data));
      } catch (e) {
        console.error("❌ Lỗi parse JSON:", e.message);
        return;
      }

      const { queryType, params } = request;

      let result;
      try {
        if (queryType === "BATCH_GET_COUNTERS") {
          result = await handleBatchCounterQuery(params);
        } else {
          result = await handleQuery(queryType, params);
        }
      } catch (error) {
        console.error(`❌ Lỗi xử lý ${queryType}:`, error.message);
        result = { success: false, error: error.message };
      }

      msg.respond(sc.encode(JSON.stringify(result)));
      console.log(`📤 Trả lời: ${queryType} -> success=${result.success}`);
    },
  });

  nc.subscribe("db.batch.query", {
    queue: "db-workers",
    callback: async (err, msg) => {
      if (err) {
        console.error("❌ Lỗi nhận batch message:", err);
        return;
      }

      let request;
      try {
        request = JSON.parse(sc.decode(msg.data));
      } catch (e) {
        console.error("❌ Lỗi parse batch JSON:", e.message);
        msg.respond(sc.encode(JSON.stringify({ success: false, error: e.message })));
        return;
      }

      const { queries } = request;
      if (!Array.isArray(queries)) {
        msg.respond(sc.encode(JSON.stringify({ success: false, error: "queries must be array" })));
        return;
      }

      const results = [];
      for (const q of queries) {
        try {
          const result = await handleQuery(q.queryType, q.params);
          results.push(result);
        } catch (error) {
          results.push({ success: false, error: error.message, queryType: q.queryType });
        }
      }

      msg.respond(sc.encode(JSON.stringify({ success: true, results })));
      console.log(`📤 Batch trả lời: ${queries.length} queries`);
    },
  });

  console.log("⏳ DB Service ready, chờ request...\n");
}

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received, shutting down DB service...");
  if (nc) await nc.drain();
  for (const c of scyllaClients) {
    try {
      await c.shutdown();
    } catch (e) {}
  }
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("SIGINT received, shutting down DB service...");
  if (nc) await nc.drain();
  for (const c of scyllaClients) {
    try {
      await c.shutdown();
    } catch (e) {}
  }
  process.exit(0);
});

startDBService().catch(console.error);
