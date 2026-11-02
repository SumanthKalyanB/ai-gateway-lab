"""Task 2: a small, authenticated HTTP/JSON-RPC MCP gateway.

This assessment proxy supports JSON responses, not downstream SSE sessions.
The downstream URL is deployment configuration, never user input.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx
import jwt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


@dataclass(frozen=True)
class ProxyConfig:
    downstream_url: str
    jwt_secret: str
    issuer: str = "assessment-auth"
    audience: str = "mcp-gateway"
    downstream_token: str | None = None
    allowed_origins: tuple[str, ...] = ()
    max_body_bytes: int = 1_048_576
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if len(self.jwt_secret.encode()) < 32:
            raise ValueError("MCP_JWT_SECRET must contain at least 32 bytes")
        parts = urlsplit(self.downstream_url)
        if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password or parts.fragment:
            raise ValueError("Invalid downstream URL")
        if self.max_body_bytes < 1 or self.timeout_seconds <= 0:
            raise ValueError("Body limit and timeout must be positive")

    @classmethod
    def from_env(cls) -> "ProxyConfig":
        return cls(
            downstream_url=os.environ.get("MCP_DOWNSTREAM_URL", "http://127.0.0.1:9000/mcp"),
            jwt_secret=os.environ["MCP_JWT_SECRET"],
            issuer=os.environ.get("MCP_JWT_ISSUER", "assessment-auth"),
            audience=os.environ.get("MCP_JWT_AUDIENCE", "mcp-gateway"),
            downstream_token=os.environ.get("MCP_DOWNSTREAM_TOKEN"),
            allowed_origins=tuple(filter(None, os.environ.get("MCP_ALLOWED_ORIGINS", "").split(","))),
        )


def _error(request_id: str | int | None, code: int, message: str, status: int = 200, *, notification: bool = False) -> Response:
    if notification:
        return Response(status_code=204)
    return JSONResponse({"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}, status_code=status)


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate JSON key")
        value[key] = item
    return value


def _reject_constant(_: str) -> None:
    raise ValueError("Non-finite JSON number")


def create_app(config: ProxyConfig | None = None, client: httpx.AsyncClient | None = None) -> FastAPI:
    config = config or ProxyConfig.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if client is not None:
            app.state.downstream = client
            yield
        else:
            async with httpx.AsyncClient(timeout=config.timeout_seconds, follow_redirects=False, trust_env=False) as owned:
                app.state.downstream = owned
                yield

    app = FastAPI(title="MCP Security Gateway", lifespan=lifespan)
    # Injection also works in tests without depending on ASGI lifespan helpers.
    if client is not None:
        app.state.downstream = client

    @app.post("/mcp")
    async def proxy(request: Request) -> Response:
        origin = request.headers.get("origin")
        if origin is not None and origin not in config.allowed_origins:
            return _error(None, -32001, "Origin not allowed", 403)
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            return _error(None, -32600, "Content-Type must be application/json", 415)
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > config.max_body_bytes:
                return _error(None, -32600, "Request too large", 413)
            body.extend(chunk)
        try:
            message = json.loads(bytes(body), object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        except (ValueError, UnicodeDecodeError, RecursionError):
            return _error(None, -32700, "Parse error", 400)
        # MCP 2025-06-18 and later require one message per POST, not a batch.
        if not isinstance(message, dict):
            return _error(None, -32600, "Invalid Request", 400)
        request_id = message.get("id")
        if "id" in message and (isinstance(request_id, bool) or not isinstance(request_id, (str, int))):
            return _error(None, -32600, "Invalid Request", 400)
        notification = "id" not in message
        if message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str) or not message["method"]:
            return _error(request_id, -32600, "Invalid Request", 400)
        params = message.get("params", {})
        if not isinstance(params, dict):
            return _error(request_id, -32602, "Invalid params", notification=notification)

        authorization = request.headers.get("authorization", "").split()
        try:
            if len(authorization) != 2 or authorization[0].lower() != "bearer":
                raise jwt.InvalidTokenError()
            claims = jwt.decode(
                authorization[1], config.jwt_secret, algorithms=["HS256"],
                issuer=config.issuer, audience=config.audience,
                options={"require": ["exp", "iat", "sub", "iss", "aud"]},
            )
            if claims.get("role") not in ("admin", "viewer"):
                raise jwt.InvalidTokenError()
        except jwt.InvalidTokenError:
            response = _error(request_id, -32001, "Invalid bearer token", 401, notification=notification)
            response.headers["WWW-Authenticate"] = "Bearer"
            return response

        if message["method"] == "tools/call":
            name = params.get("name")
            if not isinstance(name, str) or not name or ("arguments" in params and not isinstance(params["arguments"], dict)):
                return _error(request_id, -32602, "Invalid params", notification=notification)
            if name.startswith("admin_") and claims["role"] != "admin":
                return _error(request_id, -32001, "Unauthorized Tool Call", notification=notification)

        headers = {"content-type": "application/json", "accept": "application/json"}
        for key in ("mcp-protocol-version", "mcp-session-id"):
            if key in request.headers:
                headers[key] = request.headers[key]
        if config.downstream_token:
            headers["authorization"] = f"Bearer {config.downstream_token}"
        downstream = request.app.state.downstream
        try:
            # A total deadline covers both connection setup and body consumption.
            import asyncio

            async with asyncio.timeout(config.timeout_seconds):
                async with downstream.stream("POST", config.downstream_url, content=bytes(body), headers=headers) as upstream:
                    if upstream.headers.get("content-type", "").split(";", 1)[0].strip() == "text/event-stream":
                        return _error(request_id, -32002, "Downstream streaming is not supported", 502, notification=notification)
                    response_body = bytearray()
                    async for chunk in upstream.aiter_bytes():
                        if len(response_body) + len(chunk) > config.max_body_bytes:
                            return _error(request_id, -32002, "Downstream response too large", 502, notification=notification)
                        response_body.extend(chunk)
                    response_headers = {key: upstream.headers[key] for key in ("content-type", "mcp-session-id", "mcp-protocol-version") if key in upstream.headers}
                    if notification:
                        return Response(status_code=202)
                    return Response(bytes(response_body), status_code=upstream.status_code, headers=response_headers)
        except (httpx.HTTPError, TimeoutError):
            return _error(request_id, -32002, "Downstream unavailable", 502, notification=notification)

    return app
