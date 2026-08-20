import asyncio
import nats
from aiohttp import web
import uuid
import json

NATS_URL = "nats://nats:4222"
SUBJECT = "registration.requests"

async def handle(request):
    data = await request.json()
    req_id = data.get("idempotency_key", str(uuid.uuid4()))
    # Gửi vào NATS
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    await js.publish(SUBJECT, json.dumps(data).encode())
    await nc.close()
    return web.json_response({"status": "accepted", "request_id": req_id}, status=202)

app = web.Application()
app.router.add_post('/api/registration', handle)

if __name__ == '__main__':
    web.run_app(app, port=8080)