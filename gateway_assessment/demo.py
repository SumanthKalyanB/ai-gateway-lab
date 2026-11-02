"""Local, deterministic upstreams. No real customer data, refunds, or model calls."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from typing import AsyncIterator

import jwt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

app = FastAPI(title="Assessment mock upstreams")
SAMPLE = (
    "Hello café. Contact alex@example.com. "
    "SSN: 123-45-6789. Card: 4111 1111 1111 1111. End of demo."
)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "mode": "mock"}


@app.post("/mcp")
async def mcp_mock(request: Request) -> Response:
    data = await request.json()
    method = data.get("method")
    if "id" not in data:
        return Response(status_code=204)
    if method == "initialize":
        result = {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock-downstream", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {"tools": [
            {"name": name, "inputSchema": {"type": "object", "properties": {}}}
            for name in ("public_status", "admin_reset_key")
        ]}
    elif method == "tools/call":
        name = data.get("params", {}).get("name")
        if name not in {"public_status", "admin_reset_key"}:
            return JSONResponse({"jsonrpc": "2.0", "id": data["id"], "error": {
                "code": -32602, "message": "Unknown tool",
            }})
        result = {"content": [{"type": "text", "text": f"Mock tool executed: {name}"}],
                  "isError": False}
    elif method == "ping":
        result = {}
    else:
        return JSONResponse({"jsonrpc": "2.0", "id": data["id"], "error": {
            "code": -32601, "message": "Method not found",
        }})
    return JSONResponse({"jsonrpc": "2.0", "id": data["id"], "result": result})


def event(data: dict | str) -> bytes:
    value = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    return f"data: {value}\n\n".encode()


async def completions(request: Request) -> Response:
    body = await request.json()
    limit = body.get("max_tokens", body.get("max_completion_tokens", 256))
    text = SAMPLE[:limit]  # Deterministic demo only; this is not a model tokenizer.
    usage = {"prompt_tokens": 12, "completion_tokens": min(len(text), limit)}
    usage["total_tokens"] = sum(usage.values())
    if body.get("stream"):
        async def chunks() -> AsyncIterator[bytes]:
            for start in range(0, len(text), 3):
                yield event({"id": "mock-stream", "object": "chat.completion.chunk",
                             "choices": [{"index": 0, "delta": {"content": text[start:start + 3]},
                                          "finish_reason": None}]})
                await asyncio.sleep(0.01)
            yield event({"id": "mock-stream", "object": "chat.completion.chunk",
                         "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
            yield event("[DONE]")
        return StreamingResponse(chunks(), media_type="text/event-stream")
    return JSONResponse({"id": "mock-completion", "object": "chat.completion",
                         "choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                                      "finish_reason": "stop"}], "usage": usage})


app.add_api_route("/v1/chat/completions", completions, methods=["POST"])


@app.post("/primary429/v1/chat/completions")
async def rate_limited() -> JSONResponse:
    return JSONResponse({"error": "Mock primary quota reached"}, status_code=429)


@app.post("/slow/v1/chat/completions")
async def slow(request: Request) -> Response:
    await asyncio.sleep(3.5)
    return await completions(request)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="Run mock MCP and LLM upstreams on loopback")
    serve.add_argument("--port", type=int, default=9000)
    token = sub.add_parser("token", help="Sign a short-lived local test JWT")
    token.add_argument("role", choices=["admin", "viewer"])
    args = parser.parse_args()
    if args.command == "serve":
        import uvicorn
        uvicorn.run(app, host="127.0.0.1", port=args.port)
    else:
        secret = os.environ.get("MCP_JWT_SECRET", "")
        if len(secret.encode()) < 32:
            parser.error("MCP_JWT_SECRET must contain at least 32 bytes")
        now = int(time.time())
        print(jwt.encode({"sub": "local-demo", "role": args.role, "iat": now,
                          "exp": now + 900,
                          "iss": os.environ.get("MCP_JWT_ISSUER", "assessment-auth"),
                          "aud": os.environ.get("MCP_JWT_AUDIENCE", "mcp-gateway")},
                         secret, algorithm="HS256"))


if __name__ == "__main__":
    main()
