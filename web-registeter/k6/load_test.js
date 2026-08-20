// ============================================================
// k6-performance-test.js -- TIÊM TẢI TOPSIS-HYBRID (api-service)
// ------------------------------------------------------------
// Bản chính của web-registeter/k6 (thay cho load_test cũ), dựa trên
// chiến thuật: warmup nhẹ -> đỉnh 750 VU -> rút quân dần.
//
// CÁC ENDPOINT DƯỚI ĐÂY TRÙNG 100% VỚI ROUTE THẬT TRONG api.go:
//   api.go: r.Group("/DangKyHocPhan")  +  r.GET("/", health)
//   ------------------------------------------------------------------
//   k6 scenario                     | api.go route               | params (query/body)
//   ------------------------------------------------------------------
//   health                          | GET  /                     | -
//   GET_CHI_TIET_LOP_HOC_PHAN       | GET  /DangKyHocPhan/GetChiTietLopHocPhan       | idLopHocPhan
//   GET_DANH_SACH_LOP_HOC_PHAN      | GET  /DangKyHocPhan/GetDanhSachLopHocPhan      | TenMonHoc
//   GET_DANH_SACH_MON_HOC_PHAN_DANG_KY | GET /DangKyHocPhan/GetDanhSachMonHocPhanDangKy | masinhvien,dotDangKy,hinhThuc
//   BatchGetCounters                | GET  /DangKyHocPhan/BatchGetCounters          | maLopHocPhans (lặp)
//   TrangThaiDangKy                 | GET  /DangKyHocPhan/TrangThaiDangKy           | maDangKy
//   DANG_KY_MON_HOC                 | POST /DangKyHocPhan/DangKyMonHoc              | maSinhVien,maLopHocPhan,maLopHocPhanPhu,dotDangKy,hinhThuc
//   HUY_DANG_KY                     | POST /DangKyHocPhan/HuyDangKy                 | maSinhVien,maLopHocPhan
//   ------------------------------------------------------------------
//
// Điểm nổi bật:
//   - Custom metrics cache_hit_rate / cache_miss_rate /
//     cache_response_time / db_response_time (Redis nội bộ + Cloudflare).
//   - VU context: mỗi người dùng ảo giữ lịch sử truy vấn/đăng ký để
//     tăng cache-hit thực tế + hủy đúng bản ghi mình đã đăng ký.
//   - GET cho API tra cứu (cache), POST cho đăng ký/hủy (ghi).
//   - DANG_KY_MON_HOC có maLopHocPhanPhu -> test đúng đường HYBRID.
//
// Dữ liệu SEED khớp dữ liệu trên Scylla Cloud của hệ thống:
//   mon_hoc:      MH000..MH059 (60 môn)
//   sinh_vien:    SV000000..SV004999 (5000 SV)
//   lop_hoc_phan: MH###-L0..L2 (180 lớp)
//
// Chạy:
//   k6 run --out json=out/k6_raw.json load_test.js
//   python3 extract_network.py out/k6_raw.json out/network_metrics.csv
// ============================================================
import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";
import { SharedArray } from "k6/data";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.2/index.js";

// ============================================
// CUSTOM METRICS (tự xuất hiện trong báo cáo mặc định)
// ============================================
const cacheHitRate = new Rate("cache_hit_rate");
const cacheMissRate = new Rate("cache_miss_rate");
const cacheResponseTime = new Trend("cache_response_time");
const dbResponseTime = new Trend("db_response_time");

// ============================================
// LOAD SEED DATA (khớp với DB thật)
// ============================================
const MON_HOC_IDS = new SharedArray("mon_hoc", function () {
  const ids = [];
  for (let i = 0; i < 60; i++) {
    ids.push(`MH${String(i).padStart(3, "0")}`);
  }
  return ids;
});

const SINH_VIEN_IDS = new SharedArray("sinh_vien", function () {
  const ids = [];
  for (let i = 0; i < 5000; i++) {
    ids.push(`SV${String(i).padStart(6, "0")}`);
  }
  return ids;
});

const LOP_HOC_PHAN_IDS = new SharedArray("lop_hoc_phan", function () {
  const ids = [];
  for (let c = 0; c < 60; c++) {
    const mon = `MH${String(c).padStart(3, "0")}`;
    for (let s = 0; s < 3; s++) {
      ids.push(`${mon}-L${s}`);
    }
  }
  return ids;
});

// Lớp khác CÙNG môn để làm nguyện vọng phụ (HYBRID)
function altSectionFor(lhp) {
  const m = lhp.slice(0, 5); // MH###
  const l = parseInt(lhp.slice(-1));
  return `${m}-L${(l + 1) % 3}`;
}

// ============================================
// CẤU HÌNH TEST (tối ưu 750 VU đỉnh; env để dùng ít hơn)
// ============================================
const PEAK = Math.round(Number(__ENV.PEAK_VU || 750));
export const options = {
  stages: [
    { duration: "30s", target: 10 }, // Khởi động nhẹ để K8s/Cloudflare nhận diện
    { duration: "1m", target: PEAK }, // Đỉnh ngâm tải
    { duration: "1m", target: Math.round(PEAK * 0.66) }, // Rút quân
    { duration: "2m", target: Math.round(PEAK * 0.26) }, // Rút quân
    { duration: "3m", target: Math.max(1, Math.round(PEAK * 0.13)) }, // Rút quân
    { duration: "10m", target: 0 }, // Rút quân hết
  ],
  thresholds: {
    http_req_duration: ["p(50)<500", "p(90)<1000", "p(95)<2000", "p(99)<5000"],
    http_req_failed: ["rate<0.05"],
    cache_hit_rate: ["rate>0.5"], // Cloudflare/Redis gánh > 50% tải
    cache_response_time: ["p(95)<200"],
    db_response_time: ["p(95)<3000"],
  },
};

// BASE_URL: mặc định api-service local (:4000); hệ thống triển khai thì đặt
// qua env, ví dụ: BASE_URL=https://api.vanhstack.dev ./run.sh
const BASE_URL = __ENV.BASE_URL || "http://localhost:4000";

// ============================================
// TEST SCENARIOS
// ============================================
const SCENARIOS = {
  GET_CHI_TIET_LOP_HOC_PHAN: {
    weight: 30,
    endpoint: "/DangKyHocPhan/GetChiTietLopHocPhan",
    cacheable: true,
    buildParams: () => ({
      idLopHocPhan:
        LOP_HOC_PHAN_IDS[Math.floor(Math.random() * LOP_HOC_PHAN_IDS.length)],
    }),
  },

  GET_DANH_SACH_MON_HOC_PHAN_DANG_KY: {
    weight: 30,
    endpoint: "/DangKyHocPhan/GetDanhSachMonHocPhanDangKy",
    cacheable: true,
    buildParams: () => ({
      masinhvien:
        SINH_VIEN_IDS[Math.floor(Math.random() * SINH_VIEN_IDS.length)],
      hinhThuc: "Chinh quy",
    }),
  },

  GET_DANH_SACH_LOP_HOC_PHAN: {
    weight: 35,
    endpoint: "/DangKyHocPhan/GetDanhSachLopHocPhan",
    cacheable: true,
    buildParams: () => ({
      TenMonHoc: MON_HOC_IDS[Math.floor(Math.random() * MON_HOC_IDS.length)],
    }),
  },

  DANG_KY_MON_HOC: {
    weight: 5,
    endpoint: "/DangKyHocPhan/DangKyMonHoc",
    cacheable: false,
    buildParams: () => {
      const sv =
        SINH_VIEN_IDS[
          Math.floor(Math.random() * Math.min(SINH_VIEN_IDS.length, 1000))
        ];
      const lhp =
        LOP_HOC_PHAN_IDS[Math.floor(Math.random() * LOP_HOC_PHAN_IDS.length)];
      return {
        maSinhVien: sv,
        maLopHocPhan: lhp,
        // Nguyện vọng PHỤ cùng môn -> AllocationOptimizer chạy chế độ HYBRID
        maLopHocPhanPhu: altSectionFor(lhp),
        hinhThuc: "Chinh quy",
      };
    },
  },

  HUY_DANG_KY: {
    weight: 0, // tắt mặc định; bật khi muốn đo luôn hủy (dùng lịch sử đăng ký của VU)
    endpoint: "/DangKyHocPhan/HuyDangKy",
    cacheable: false,
    buildParams: () => {
      const sv =
        SINH_VIEN_IDS[
          Math.floor(Math.random() * Math.min(SINH_VIEN_IDS.length, 1000))
        ];
      const lhp =
        LOP_HOC_PHAN_IDS[
          Math.floor(Math.random() * Math.min(LOP_HOC_PHAN_IDS.length, 100))
        ];
      return {
        maSinhVien: sv,
        maLopHocPhan: lhp,
      };
    },
  },
};

const TOTAL_WEIGHT = Object.values(SCENARIOS).reduce(
  (sum, s) => sum + s.weight,
  0,
);

// ============================================
// VU CONTEXT
// ============================================
const vuContexts = new Map();

function getVUContext() {
  const vuId = __VU;
  if (!vuContexts.has(vuId)) {
    vuContexts.set(vuId, {
      requestCount: 0,
      recentSinhViens: [],
      recentLopHocPhans: [],
      dangKyHistory: [],
    });
  }
  return vuContexts.get(vuId);
}

// ============================================
// SELECT SCENARIO
// ============================================
function selectScenario(vu) {
  if (vu.requestCount < 10) {
    const warmupScenarios = [
      SCENARIOS.GET_CHI_TIET_LOP_HOC_PHAN,
      SCENARIOS.GET_DANH_SACH_MON_HOC_PHAN_DANG_KY,
      SCENARIOS.GET_DANH_SACH_LOP_HOC_PHAN,
    ];
    return warmupScenarios[vu.requestCount % warmupScenarios.length];
  }

  let rand = Math.random() * TOTAL_WEIGHT;
  for (const [name, scenario] of Object.entries(SCENARIOS)) {
    rand -= scenario.weight;
    if (rand <= 0) return scenario;
  }

  return SCENARIOS.GET_CHI_TIET_LOP_HOC_PHAN;
}

// ============================================
// MAIN TEST FUNCTION
// ============================================
export default function () {
  const vu = getVUContext();

  const scenario = selectScenario(vu);
  let params = scenario.buildParams();

  // Tăng cache hit: 40% dùng lại dữ liệu gần đây
  if (Math.random() < 0.4 && vu.recentSinhViens.length > 0) {
    if (params.masinhvien || params.maSinhVien) {
      const key = params.masinhvien ? "masinhvien" : "maSinhVien";
      params[key] =
        vu.recentSinhViens[
          Math.floor(Math.random() * vu.recentSinhViens.length)
        ];
    }
    if (params.idLopHocPhan || params.maLopHocPhan) {
      const key = params.idLopHocPhan ? "idLopHocPhan" : "maLopHocPhan";
      params[key] =
        vu.recentLopHocPhans[
          Math.floor(Math.random() * vu.recentLopHocPhans.length)
        ];
    }
  }

  // Lưu vào recent history
  if (params.masinhvien && vu.recentSinhViens.length < 20) {
    vu.recentSinhViens.push(params.masinhvien);
  }
  if (params.maSinhVien && vu.recentSinhViens.length < 20) {
    vu.recentSinhViens.push(params.maSinhVien);
  }
  if (params.idLopHocPhan && vu.recentLopHocPhans.length < 20) {
    vu.recentLopHocPhans.push(params.idLopHocPhan);
  }
  if (params.maLopHocPhan && vu.recentLopHocPhans.length < 20) {
    vu.recentLopHocPhans.push(params.maLopHocPhan);
  }

  // Lưu lịch sử đăng ký
  if (scenario === SCENARIOS.DANG_KY_MON_HOC) {
    vu.dangKyHistory.push({ ...params });
    if (vu.dangKyHistory.length > 10) {
      vu.dangKyHistory.shift();
    }
  }

  // Hủy đăng ký dùng lịch sử cũ
  if (
    scenario === SCENARIOS.HUY_DANG_KY &&
    vu.dangKyHistory.length > 0 &&
    Math.random() < 0.7
  ) {
    const historyItem =
      vu.dangKyHistory[Math.floor(Math.random() * vu.dangKyHistory.length)];
    params = { ...historyItem };
  }

  // Thực hiện request
  executeRequest(scenario.endpoint, params, scenario.cacheable);

  vu.requestCount++;
  sleep(0.1 + Math.random() * 0.8);
}

// ============================================
// EXECUTE REQUEST (GET cho cacheable, POST cho mutation)
// ============================================
function executeRequest(endpoint, params, cacheable) {
  let url = `${BASE_URL}${endpoint}`;
  let response;

  const tags = {
    endpoint: endpoint.split("/").pop(),
    cacheable: String(cacheable),
  };

  const headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
  };

  if (cacheable) {
    // API tra cứu -> params thành query string + GET (đi qua cache Redis/Edge)
    const queryParams = Object.keys(params)
      .map((k) => `${encodeURIComponent(k)}=${encodeURIComponent(params[k])}`)
      .join("&");

    if (queryParams.length > 0) {
      url = `${url}?${queryParams}`;
    }

    response = http.get(url, {
      headers: headers,
      timeout: "10s",
      tags: tags,
    });
  } else {
    // API ghi -> POST + body JSON
    const payload = JSON.stringify(params);
    response = http.post(url, payload, {
      headers: headers,
      timeout: "5s",
      tags: tags,
    });
  }

  const duration = response.timings.duration;
  if (response.status !== 200) {
    console.log(
      `Lỗi gòi: Status ${response.status} - Body: ${response.body.substring(0, 100)}`,
    );
  }

  const result = check(response, {
    "HTTP 200": (r) => r.status === 200,
    "Business success": (r) => {
      try {
        return JSON.parse(r.body).success === true;
      } catch (e) {
        return false;
      }
    },
  });

  // Ghi custom metrics
  if (result) {
    let isCacheHit = false;

    // 1. Cloudflare Edge gánh đạn?
    const cfCacheStatus = response.headers["Cf-Cache-Status"];
    if (cfCacheStatus === "HIT") {
      isCacheHit = true;
    } else {
      // 2. Redis nội bộ (api-service cache) đỡ được không?
      try {
        isCacheHit = JSON.parse(response.body).fromCache === true;
      } catch (e) {
        // ignore
      }
    }

    if (isCacheHit) {
      cacheHitRate.add(true, tags);
      cacheResponseTime.add(duration, tags);
    } else {
      dbResponseTime.add(duration, tags);
      if (cacheable) {
        cacheMissRate.add(true, tags); // chỉ tính miss trên các request cacheable
      }
    }
  }
}

export function handleSummary(data) {
  return {
    stdout: textSummary(data, { indent: " ", enableColors: true }),
    "summary.json": JSON.stringify(data),
  };
}