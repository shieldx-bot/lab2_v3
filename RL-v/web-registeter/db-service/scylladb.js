const cassandra = require("cassandra-driver");
const { v4: uuidv4 } = require("uuid");

const list = ["192.168.24.2", "192.168.24.3", "192.168.24.6"];
const clients = [];
const latencies = [];

// Đọc tham số dòng lệnh
const args = process.argv.slice(2);
const targetRps = 100;
const durationSec = args[1] ? parseInt(args[1], 10) : 30;

if (isNaN(targetRps) || targetRps <= 0) {
  console.error(
    "❌ RPS không hợp lệ, sử dụng: node script.js <rps> <duration_giay>",
  );
  process.exit(1);
}

const intervalMs = 1000 / targetRps; // ví dụ 100 rps -> 10ms

// Khởi tạo pool kết nối
async function initPool() {
  for (const ip of list) {
    const client = new cassandra.Client({
      contactPoints: [ip],
      localDataCenter: "datacenter1",
      keyspace: "my_keyspace",
    });
    await client.connect();
    clients.push(client);
    console.log(`✅ Đã kết nối đến ${ip}`);
  }
  console.log(`📦 Pool size: ${clients.length}`);
}

async function sendRequest() {
  const idx = Math.floor(Math.random() * clients.length);
  const client = clients[idx];
  const id = uuidv4();
  const start = Date.now();

  try {
    await client.execute(
      "INSERT INTO users (user_id, first_name, last_name, age) VALUES (?, ?, ?, ?)",
      [id, "value_0", "value_1", 1],
      { prepare: true },
    );
    await client.execute("SELECT * FROM users WHERE user_id = ?", [id], {
      prepare: true,
    });
    const latency = Date.now() - start;
    latencies.push(latency);
    console.log(`✔️  OK id: ${id}, latency: ${latency}ms`);
  } catch (err) {
    console.error(`❌ Lỗi id ${id}:`, err.message);
  }
}

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const index = (p / 100) * (sorted.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];
  return sorted[lower] * (upper - index) + sorted[upper] * (index - lower);
}

async function startBenchmark() {
  await initPool();
  console.log(
    `🚀 Bắt đầu benchmark: ${targetRps} req/s trong ${durationSec}s (interval = ${intervalMs.toFixed(2)}ms)`,
  );

  let counter = 0;
  const startTime = Date.now();

  const interval = setInterval(() => {
    sendRequest().catch((err) => console.error("Unexpected:", err));
    counter++;
  }, intervalMs);

  setTimeout(() => {
    clearInterval(interval);
    const elapsed = (Date.now() - startTime) / 1000;
    console.log(`\n⏹️  Đã dừng benchmark.`);
    console.log(`📊 Đã gửi ~${counter} requests trong ${elapsed.toFixed(1)}s`);
    console.log(`🔥 Tốc độ thực tế: ~${(counter / elapsed).toFixed(1)} req/s`);

    if (latencies.length > 0) {
      const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      const p95 = percentile(latencies, 95);
      console.log(
        `⏱️  Latency (chỉ thành công) - avg: ${avg.toFixed(2)}ms, p95: ${p95.toFixed(2)}ms`,
      );
    } else {
      console.log(`⚠️  Không có request thành công.`);
    }

    clients.forEach((c) => c.shutdown());
    console.log("👋 Đã đóng toàn bộ kết nối.");
    process.exit(0);
  }, durationSec * 1000);
}

startBenchmark();
