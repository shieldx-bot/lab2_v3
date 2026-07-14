import express from "express";
import { connect, StringCodec } from "nats";
import Redis from "ioredis";

const app = express();
app.use(express.json());

const sc = StringCodec();
let nc;
let rdb_w = null;
let rdb_r = [];

// ============================================
// REDIS INIT
// ============================================
async function initRedis() {
  const writeIP = "192.168.24.3";
  const readIPs = ["192.168.24.2", "192.168.24.6"];

  for (const ip of readIPs) {
    try {
      const redis = new Redis({
        host: ip,
        port: 6379,
        maxRetriesPerRequest: 5,
        enableReadyCheck: true,
        retryStrategy: (times) => Math.min(times * 50, 2000),
      });
      await redis.ping();
      rdb_r.push(redis);
      console.log(`✅ Redis READ: ${ip}`);
    } catch (err) {
      console.error(`❌ Redis READ ${ip}:`, err.message);
    }
  }

  try {
    const redis_w = new Redis({
      host: writeIP,
      port: 6379,
      maxRetriesPerRequest: 5,
      enableReadyCheck: true,
      retryStrategy: (times) => Math.min(times * 50, 2000),
    });
    await redis_w.ping();
    rdb_w = redis_w;
    console.log(`✅ Redis WRITE: ${writeIP}`);
  } catch (err) {
    console.error(`❌ Redis WRITE ${writeIP}:`, err.message);
  }
}

function getRedisRead() {
  return rdb_r.length > 0
    ? rdb_r[Math.floor(Math.random() * rdb_r.length)]
    : rdb_w;
}

function generateCacheKey(queryType, params) {
  const sortedKeys = Object.keys(params).sort();
  return `${queryType}:${sortedKeys.map((k) => `${k}:${params[k]}`).join("|")}`;
}

async function getFromCache(cacheKey) {
  try {
    const data = await getRedisRead().get(cacheKey);
    return data ? JSON.parse(data) : null;
  } catch (err) {
    return null;
  }
}

async function setToCache(cacheKey, data, ttl = 600) {
  if (!rdb_w) return;
  try {
    await rdb_w.set(cacheKey, JSON.stringify(data), "EX", ttl);
  } catch (err) {}
}

async function deleteCacheKey(cacheKey) {
  if (!rdb_w) return;
  try {
    await rdb_w.del(cacheKey);
    console.log(`🗑️ Cache deleted: ${cacheKey}`);
  } catch (err) {}
}

// ============================================
// NATS (dùng request/response chuẩn)
// ============================================
async function connectNats() {
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
}

async function sendDBRequest(queryType, params, timeoutMs = 10000) {
  const request = { queryType, params };
  try {
    const responseMsg = await nc.request(
      "db.query",
      sc.encode(JSON.stringify(request)),
      {
        timeout: timeoutMs,
      },
    );
    const result = JSON.parse(sc.decode(responseMsg.data));
    return result;
  } catch (err) {
    throw new Error(`NATS request failed: ${err.message}`);
  }
}

// ============================================
// XỬ LÝ REQUEST + CACHE (có invalidate)
// ============================================
async function handleRequestWithCache(
  queryType,
  params,
  res,
  cacheable = true,
) {
  // --- Không cache (mutation) ---
  if (!cacheable) {
    try {
      const result = await sendDBRequest(queryType, params);
      // Sau mutation thành công → xóa các cache liên quan
      if (result.success) {
        // Xóa cache danh sách đăng ký của sinh viên này
        if (params.maSinhVien) {
          const listKey = generateCacheKey(
            "GET_DANH_SACH_MON_HOC_PHAN_DANG_KY",
            {
              masinhvien: params.maSinhVien,
              dotDangKy: params.dotDangKy || "",
              hinhThuc: params.hinhThuc || "",
            },
          );
          await deleteCacheKey(listKey);
        }
        // Xóa cache chi tiết lớp học phần nếu có
        if (params.maLopHocPhan) {
          const detailKey = generateCacheKey("GET_CHI_TIET_LOP_HOC_PHAN", {
            idLopHocPhan: params.maLopHocPhan,
          });
          await deleteCacheKey(detailKey);
        }
        // Nếu là hủy đăng ký, cũng nên xóa cache danh sách tương tự
      }
      return res.json(result);
    } catch (err) {
      return res.status(504).json({ error: err.message });
    }
  }

  // --- Có cache (query) ---
  const cacheKey = generateCacheKey(queryType, params);
  const cachedData = await getFromCache(cacheKey);
  if (cachedData) {
    console.log(`💾 Cache HIT: ${queryType}`);
    return res.json({ success: true, data: cachedData, fromCache: true });
  }

  console.log(`🔄 Cache MISS: ${queryType}`);
  try {
    const result = await sendDBRequest(queryType, params);
    if (result.success && result.data) {
      await setToCache(cacheKey, result.data);
    }
    return res.json(result);
  } catch (err) {
    // Retry 1 lần nếu timeout
    try {
      const result = await sendDBRequest(queryType, params, 15000);
      if (result.success && result.data) {
        await setToCache(cacheKey, result.data);
      }
      return res.json(result);
    } catch (retryErr) {
      return res.status(504).json({ error: retryErr.message });
    }
  }
}

// ============================================
// ROUTES
// ============================================
async function startAPIService() {
  await initRedis();
  await connectNats();

  app.get("/", (req, res) => res.json({ message: "API healthy" }));

  // 1. Chuyển sang GET và lấy data từ req.query
  app.get("/DangKyHocPhan/GetChiTietLopHocPhan", async (req, res) => {
    const { idLopHocPhan } = req.query; // Đổi req.body thành req.query
    if (!idLopHocPhan)
      return res.status(400).json({ error: "Thiếu idLopHocPhan" });
    await handleRequestWithCache(
      "GET_CHI_TIET_LOP_HOC_PHAN",
      { idLopHocPhan },
      res,
      true,
    );
  });

  // 2. Chuyển sang GET
  app.get("/DangKyHocPhan/GetDanhSachMonHocPhanDangKy", async (req, res) => {
    const { masinhvien, dotDangKy, hinhThuc } = req.query;
    if (!masinhvien) return res.status(400).json({ error: "Thiếu masinhvien" });
    await handleRequestWithCache(
      "GET_DANH_SACH_MON_HOC_PHAN_DANG_KY",
      { masinhvien, dotDangKy, hinhThuc },
      res,
      true,
    );
  });

  // 3. Chuyển sang GET
  app.get("/DangKyHocPhan/GetDanhSachLopHocPhan", async (req, res) => {
    const { TenMonHoc } = req.query;
    if (!TenMonHoc) return res.status(400).json({ error: "Thiếu TenMonHoc" });
    await handleRequestWithCache(
      "GET_DANH_SACH_LOP_HOC_PHAN",
      { TenMonHoc },
      res,
      true,
    );
  });

  // 4. API Đăng ký (Ghi dữ liệu) -> BẮT BUỘC GIỮ LẠI POST
  app.post("/DangKyHocPhan/DangKyMonHoc", async (req, res) => {
    const { maSinhVien, maLopHocPhan, dotDangKy, hinhThuc } = req.body;
    if (!maSinhVien || !maLopHocPhan)
      return res.status(400).json({ error: "Thiếu thông tin" });
    await handleRequestWithCache(
      "DANG_KY_MON_HOC",
      { maSinhVien, maLopHocPhan, dotDangKy, hinhThuc },
      res,
      false, // Không cache
    );
  });

  // 5. API Hủy Đăng ký (Ghi dữ liệu) -> BẮT BUỘC GIỮ LẠI POST
  app.post("/DangKyHocPhan/HuyDangKy", async (req, res) => {
    const { maSinhVien, maLopHocPhan } = req.body;
    if (!maSinhVien || !maLopHocPhan)
      return res.status(400).json({ error: "Thiếu thông tin" });
    await handleRequestWithCache(
      "HUY_DANG_KY",
      { maSinhVien, maLopHocPhan },
      res,
      false, // Không cache
    );
  });

  app.listen(3000, () => console.log("🚀 API Service listening on :3000"));
}

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received, closing API service...");
  if (nc) await nc.drain();
  if (rdb_w) await rdb_w.quit();
  for (const r of rdb_r) await r.quit();
  process.exit(0);
});

startAPIService().catch(console.error);
