import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'https://dangky.university.edu.vn';
const VUS = __ENV.VUS || 1000;
const METHOD = __ENV.METHOD || 'M4';

export const options = {
  stages: [
    { duration: '2m', target: VUS },
    { duration: '10m', target: VUS },
    { duration: '1m', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // cảnh báo nếu >500ms
  },
};

export default function () {
  const idempotencyKey = `${__VU}-${__ITER}-${uuidv4()}`;
  const payload = JSON.stringify({
    student_id: `SV${__VU}`,  // giả định VU number map với student_id
    preferences: ['SEC101', 'SEC102', 'SEC201'],
    method: METHOD,
    idempotency_key: idempotencyKey,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer dummy-token-${__VU}`,
    },
  };

  const res = http.post(`${BASE_URL}/api/registration`, payload, params);
  check(res, {
    'is accepted': (r) => r.status === 202,
  });

  // Polling trạng thái vài lần
  if (res.status === 202) {
    const reqId = res.json('request_id');
    for (let i = 0; i < 5; i++) {
      sleep(2);
      const statusRes = http.get(`${BASE_URL}/api/registration/status/${reqId}`, params);
      check(statusRes, { 'status ok': (r) => r.status === 200 });
    }
  }

  sleep(1);
}