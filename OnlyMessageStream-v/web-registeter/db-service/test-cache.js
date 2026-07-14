const Redis = require("ioredis");

const rdb_w = [];
const rdb_r = [];

async function initRedis() {
  const write = "192.168.24.3";
  const read = ["192.168.24.2", "192.168.24.6"];
  // for (const ip of read) {
  //  redis = new Redis({
  //    host: ip,
  //    port: 6379,
  //    maxRetriesPerRequest: 5,
  //    enableReadyCheck: true,
  //    retryStrategy: (times) => Math.min(times * 50, 2000)
  //  });
  //   rdb_r.push(redis)
  //  }
  redis_w = new Redis({
    host: write,
    port: 6379,
    maxRetriesPerRequest: 5,
    enableReadyCheck: true,
    retryStrategy: (times) => Math.min(times * 50, 2000),
  });
  rdb_w.push(redis_w);
}
initRedis();
console.log("success");
console.log(rdb_w[0]);
