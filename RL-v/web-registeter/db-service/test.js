// test-db.js
const { connect, StringCodec } = require("nats");
const sc = StringCodec();
(async () => {
  const nc = await connect({ servers: "nats://192.168.24.2:4222" });
  const requestId = Date.now().toString();
  const req = {
    requestId,
    queryType: "GET_CHI_TIET_LOP_HOC_PHAN",
    params: { idLopHocPhan: "a1b2c3d4-0001-4000-8000-000000000001" },
  };
  const sub = nc.subscribe(`db.response.${requestId}`);
  nc.publish("db.query", sc.encode(JSON.stringify(req)));
  console.log("Đã gửi request...");
  if (msg) {
    console.log("✅ Phản hồi:", JSON.parse(sc.decode(msg.data)));
  } else {
    console.log("❌ Timeout - DB Service không phản hồi");
  }
  nc.close();
})();
