import hashlib
import hmac
import os
from aiohttp import web

SECRET = os.getenv("JWT_SECRET", "super-secret-key")

def verify_token(token):
    # Giả lập kiểm tra token (thực tế dùng JWT)
    return token.startswith("valid-token")

async def auth_middleware(app, handler):
    async def middleware(request):
        if request.path.startswith("/api/registration"):
            token = request.headers.get("Authorization", "").replace("Bearer ", "")
            if not verify_token(token):
                return web.json_response({"error": "Unauthorized"}, status=401)
        return await handler(request)
    return middleware

async def health(request):
    return web.Response(text="OK")

app = web.Application(middlewares=[auth_middleware])
app.router.add_get('/health', health)

if __name__ == '__main__':
    web.run_app(app, port=8081)