import nats from "k6/x/nats";
import { Trend, Counter } from "k6/metrics";

const pubLatency = new Trend("pub_latency");
// Đếm số message gửi đến từng server (tag server)
const msgPerServer = new Counter("msg_per_server", ["server"]);

// Danh sách server riêng
const individualServers = [
  "nats://192.168.24.6:6222",
  "nats://192.168.24.2:4222",
  "nats://192.168.24.3:5222",
];

// Mỗi VU tạo 1 kết nối đến mỗi server → pool nhỏ, an toàn
const POOL_SIZE = individualServers.length; // 3 kết nối / VU
let pool = []; // mỗi VU có pool riêng

// Hàm tạo pool, chạy trong init context (1 lần mỗi VU)
function buildPool() {
  for (let i = 0; i < individualServers.length; i++) {
    const conn = new nats.Nats({
      servers: [individualServers[i]], // chỉ 1 server
      unsafe: true,
    });
    pool.push({ conn, server: individualServers[i] });
  }
}
buildPool(); // gọi ngay

export const options = {
  scenarios: {
    test: {
      executor: "constant-arrival-rate",
      rate: 70,
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 300,
      maxVUs: 500,
    },
  },
};

export default function () {
  // Chọn ngẫu nhiên 1 kết nối trong pool của VU này
  const idx = Math.floor(Math.random() * pool.length);
  const { conn, server } = pool[idx];

  const topic = "test.balance";

  const start = Date.now();
  conn.publish(topic, "hello " + __ITER);
  pubLatency.add(Date.now() - start);

  // Gắn tag server để theo dõi số lượng gửi đến mỗi server
  msgPerServer.add(1, { server: server });
}

export function teardown() {
  // Không cần đóng pool vì mỗi VU tự quản lý, nhưng nếu muốn dọn dẹp:
  // (teardown chạy ở global scope, không có quyền truy cập pool của từng VU)
  // => không làm gì, k6 sẽ tự dọn khi VU kết thúc.
}
