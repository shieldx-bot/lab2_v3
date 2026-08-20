import asyncio
import nats
import json

async def run():
    nc = await nats.connect("nats://nats:4222")
    js = nc.jetstream()
    sub = await js.subscribe("allocation.results", durable="notifier")
    async for msg in sub.messages:
        data = json.loads(msg.data)
        student_id = data['student_id']
        status = data['status']
        # Mô phỏng gửi email hoặc webhook
        print(f"Notification to {student_id}: registration {status}")
        await msg.ack()
    await nc.close()

if __name__ == '__main__':
    asyncio.run(run())