import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    constant_request_rate: {
      executor: 'constant-arrival-rate',
      rate: 100, // iterations per timeUnit => ~100 req/sec
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 200,
      maxVUs: 600,
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.1'],
    'http_req_duration': ['p(95)<1500'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';

function randint(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }
function pad(n, width) { return String(n).padStart(width, '0'); }
function randSinhVien() { return `SV${pad(randint(1,5000),5)}`; }
function randLopHocPhan() { return randint(1, 500); }
function randTenMonHoc() {
  const topics = [
    'Nhap mon lap trinh','Cau truc du lieu','Giai thuat nap cao',
    'Lap trinh huong doi tuong','Lap trinh web','Lap trinh di dong',
    'Co so du lieu','He quan tri CSDL','SQL nap cao',
    'Mang may tinh','An toan mang','Mang khong day',
    'He dieu hanh','He nhung','Quan tri he thong',
    'Cong nghe phan mem','Kiem thu phan mem','Phan tich thiet ke he thong',
    'Tri tue nhan tao','Hoc may','Xu ly ngon ngu tu nhien',
    'Do hoa may tinh','Thi giac may tinh','Khoa hoc du lieu'
  ];
  const t = topics[randint(0, topics.length - 1)];
  const grp = randint(1,5);
  return `${t} ${grp}`;
}

const endpoints = [
  { name: 'health', method: 'GET', path: '/' },
  { name: 'GetChiTietLopHocPhan', method: 'POST', path: '/DangKyHocPhan/GetChiTietLopHocPhan', body: () => ({ idLopHocPhan: randLopHocPhan() }) },
  { name: 'GetDanhSachMonHocPhanDangKy', method: 'POST', path: '/DangKyHocPhan/GetDanhSachMonHocPhanDangKy', body: () => ({ masinhvien: randSinhVien()}) },
  { name: 'GetDanhSachLopHocPhan', method: 'POST', path: '/DangKyHocPhan/GetDanhSachLopHocPhan', body: () => ({ TenMonHoc: randTenMonHoc() }) },
  { name: 'DangKyMonHoc', method: 'POST', path: '/DangKyHocPhan/DangKyMonHoc', body: () => ({ maSinhVien: randSinhVien(), maLopHocPhan: randLopHocPhan(), dotDangKy: `2025HK1`, hinhThuc: 'Chinh quy' }) },
];

export default function () {
  const e = endpoints[Math.floor(Math.random() * endpoints.length)];
  const url = `${BASE_URL}${e.path}`;
  let res;
  if (e.method === 'GET') {
    res = http.get(url);
  } else {
    const payload = e.body();
    Object.keys(payload).forEach(k => payload[k] === undefined && delete payload[k]);
    res = http.post(url, JSON.stringify(payload), { headers: { 'Content-Type': 'application/json' } });
  }

  check(res, { 'status < 500': (r) => r.status < 500 });
}
