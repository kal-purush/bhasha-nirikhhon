from fastapi import Request
from fastapi.openapi.models import Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.datastructures import MutableHeaders


class AuthCookieMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        jwt_token = request.cookies.get("accessToken")
        header_token = request.headers.get("Authorization")
        if jwt_token and not header_token:
            request.headers.__dict__["_list"].append(
                (b"authorization", f"Bearer {jwt_token}".encode())
            )
        response = await call_next(request)
        return response