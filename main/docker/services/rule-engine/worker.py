import asyncio
import nats
import json

async def run():
    nc = await nats.connect("nats://nats:4222")
    js = nc.jetstream()
    sub = await js.subscribe("registration.requests", durable="rule-engine")
    async for msg in sub.messages:
        data = json.loads(msg.data)
        # Kiểm tra điều kiện cứng (giả lập)
        eligible = True  # thay bằng logic thực
        if eligible:
            await js.publish("eligible.requests", json.dumps(data).encode())
        await msg.ack()
    await nc.close()

if __name__ == '__main__':
    asyncio.run(run())