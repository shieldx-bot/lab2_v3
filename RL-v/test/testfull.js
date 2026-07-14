// k6-performance-test.js
import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";
import { SharedArray } from "k6/data";

// ============================================
// CUSTOM METRICS (sẽ tự động hiển thị trong báo cáo mặc định)
// ============================================
const cacheHitRate = new Rate("cache_hit_rate");
const cacheMissRate = new Rate("cache_miss_rate");
const cacheResponseTime = new Trend("cache_response_time");
const dbResponseTime = new Trend("db_response_time");

// ============================================
// LOAD SEED DATA
// ============================================
const MON_HOC_IDS = new SharedArray("mon_hoc", function () {
  const ids = [];
  for (let i = 1; i <= 12; i++) {
    ids.push(`IT${String(i).padStart(3, "0")}`);
  }
  return ids;
});

const SINH_VIEN_IDS = new SharedArray("sinh_vien", function () {
  const ids = [];
  for (let i = 1; i <= 100; i++) {
    ids.push(`SV${String(i).padStart(5, "0")}`);
  }
  return ids;
});

const LOP_HOC_PHAN_IDS = new SharedArray("lop_hoc_phan", function () {
  const ids = [];
  for (let i = 1; i <= 50; i++) {
    ids.push(`LHP${String(i).padStart(3, "0")}`);
  }
  return ids;
});

// ============================================
// CẤU HÌNH TEST
// ============================================
export const options = {
  stages: [
    { duration: "5s", target: 100 },
    { duration: "5s", target: 1000 },
  ],

  thresholds: {
    http_req_duration: ["p(50)<500", "p(90)<1000", "p(95)<2000", "p(99)<5000"],
    http_req_failed: ["rate<0.05"],
    cache_hit_rate: ["rate>0.3"],
    cache_response_time: ["p(95)<200"],
    db_response_time: ["p(95)<3000"],
  },
};

const BASE_URL = "http://localhost:4000";

// ============================================
// TEST SCENARIOS
// ============================================
const SCENARIOS = {
  GET_CHI_TIET_LOP_HOC_PHAN: {
    weight: 25,
    endpoint: "/DangKyHocPhan/GetChiTietLopHocPhan",
    cacheable: true,
    buildParams: () => ({
      idLopHocPhan:
        LOP_HOC_PHAN_IDS[Math.floor(Math.random() * LOP_HOC_PHAN_IDS.length)],
    }),
  },

  GET_DANH_SACH_MON_HOC_PHAN_DANG_KY: {
    weight: 20,
    endpoint: "/DangKyHocPhan/GetDanhSachMonHocPhanDangKy",
    cacheable: true,
    buildParams: () => ({
      masinhvien:
        SINH_VIEN_IDS[Math.floor(Math.random() * SINH_VIEN_IDS.length)],
      hinhThuc: "Chinh quy",
    }),
  },

  GET_DANH_SACH_LOP_HOC_PHAN: {
    weight: 15,
    endpoint: "/DangKyHocPhan/GetDanhSachLopHocPhan",
    cacheable: true,
    buildParams: () => ({
      TenMonHoc: MON_HOC_IDS[Math.floor(Math.random() * MON_HOC_IDS.length)],
    }),
  },

  DANG_KY_MON_HOC: {
    weight: 30,
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
        hinhThuc: "Chinh quy",
      };
    },
  },

  HUY_DANG_KY: {
    weight: 10,
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
// EXECUTE REQUEST
// ============================================
function executeRequest(endpoint, params, cacheable) {
  const url = `${BASE_URL}${endpoint}`;
  const payload = JSON.stringify(params);

  const tags = {
    endpoint: endpoint.split("/").pop(),
    cacheable: String(cacheable),
  };

  const response = http.post(url, payload, {
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    timeout: "15s",
    tags: tags,
  });

  const duration = response.timings.duration;

  // Check response - k6 tự động ghi vào metric "checks"
  const result = check(response, {
    "HTTP 200": (r) => r.status === 200,
    "Business success": (r) => {
      try {
        return JSON.parse(r.body).success === true;
      } catch (e) {
        console.log(e);
        return false;
      }
    },
  });

  // Ghi custom metrics
  if (result) {
    let isCacheHit = false;
    try {
      isCacheHit = JSON.parse(response.body).fromCache === true;
    } catch (e) {
      // ignore
    }

    if (isCacheHit) {
      cacheHitRate.add(1, tags);
      cacheResponseTime.add(duration, tags);
    } else if (cacheable) {
      cacheMissRate.add(1, tags);
      dbResponseTime.add(duration, tags);
    } else {
      dbResponseTime.add(duration, tags);
    }
  }
}

// ============================================
// KHÔNG CẦN handleSummary - k6 tự in báo cáo mặc định
// ============================================
