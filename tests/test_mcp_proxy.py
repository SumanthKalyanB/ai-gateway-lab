from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from dataclasses import replace

import httpx
import jwt
import pytest

from gateway_assessment.mcp_proxy import ProxyConfig, create_app

CONFIG = ProxyConfig(downstream_url="http://mock/mcp", jwt_secret="test-secret-with-at-least-32-bytes-long")


def bearer(role="viewer", **overrides):
    claims = {"sub": "user-1", "role": role, "iss": CONFIG.issuer, "aud": CONFIG.audience, "iat": int(time.time()), "exp": int(time.time()) + 300}
    claims.update(overrides)
    return "Bearer " + jwt.encode(claims, CONFIG.jwt_secret, algorithm="HS256")


@asynccontextmanager
async def proxy_client(handler, config=CONFIG):
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as downstream:
        app = create_app(config, downstream)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as caller:
            yield caller


@pytest.mark.asyncio
async def test_list_is_forwarded_without_modifying_payload_or_result():
    received = []
    response_bytes = b'{ "jsonrpc": "2.0", "id": 0, "result": {"tools": [{"name":"admin_reset_key"}]} }'

    def handler(request):
        received.append(request)
        return httpx.Response(200, content=response_bytes, headers={"content-type": "application/json", "mcp-session-id": "session-1"})

    content = b'{"jsonrpc":"2.0","id":0,"method":"tools/list","params":{}}'
    async with proxy_client(handler) as client:
        response = await client.post("/mcp", content=content, headers={"authorization": bearer(), "content-type": "application/json", "mcp-protocol-version": "2025-11-25"})
    assert response.status_code == 200
    assert response.content == response_bytes
    assert response.headers["mcp-session-id"] == "session-1"
    assert len(received) == 1
    assert received[0].content == content
    assert "authorization" not in received[0].headers
    assert received[0].headers["mcp-protocol-version"] == "2025-11-25"


@pytest.mark.asyncio
async def test_viewer_admin_call_is_blocked_before_downstream_and_id_zero_survives():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={})

    async with proxy_client(handler) as client:
        response = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 0, "method": "tools/call", "params": {"name": "admin_reset_key", "arguments": {}}}, headers={"authorization": bearer()})
    assert response.json() == {"jsonrpc": "2.0", "id": 0, "error": {"code": -32001, "message": "Unauthorized Tool Call"}}
    assert not calls


@pytest.mark.asyncio
@pytest.mark.parametrize("role,name", [("admin", "admin_reset_key"), ("viewer", "get_customer_record")])
async def test_authorized_calls_forward_and_use_distinct_downstream_credential(role, name):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": "x", "result": {"ok": True}})

    async with proxy_client(handler, replace(CONFIG, downstream_token="downstream-only-secret")) as client:
        response = await client.post("/mcp", json={"jsonrpc": "2.0", "id": "x", "method": "tools/call", "params": {"name": name}}, headers={"authorization": bearer(role)})
    assert response.json()["result"]["ok"] is True
    assert calls[0].headers["authorization"] == "Bearer downstream-only-secret"


@pytest.mark.asyncio
@pytest.mark.parametrize("authorization", ["", "Bearer forged", bearer(exp=1), bearer(aud="wrong-service"), bearer(role="owner"), bearer(role=["admin"])])
async def test_bad_auth_never_reaches_downstream(authorization):
    def handler(_):
        pytest.fail("Unauthorized request reached downstream")

    async with proxy_client(handler) as client:
        response = await client.post("/mcp", json={"jsonrpc": "2.0", "id": "a", "method": "tools/list"}, headers={"authorization": authorization})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == -32001


@pytest.mark.asyncio
@pytest.mark.parametrize("body,code", [(b"broken", -32700), (b"[]", -32600), (b'{"jsonrpc":"2.0","id":true,"method":"tools/list"}', -32600), (b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":1}}', -32602), (b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x","arguments":null}}', -32602), (b'{"jsonrpc":"2.0","id":1,"id":2,"method":"tools/list"}', -32700)])
async def test_bad_wire_data_is_rejected(body, code):
    def handler(_):
        pytest.fail("Invalid request reached downstream")

    async with proxy_client(handler) as client:
        response = await client.post("/mcp", content=body, headers={"content-type": "application/json", "authorization": bearer()})
    assert response.json()["error"]["code"] == code


@pytest.mark.asyncio
async def test_blocked_notification_has_no_response_and_no_downstream_call():
    def handler(_):
        pytest.fail("Unauthorized notification reached downstream")

    async with proxy_client(handler) as client:
        response = await client.post("/mcp", json={"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "admin_reset_key"}}, headers={"authorization": bearer()})
    assert response.status_code == 204
    assert not response.content


@pytest.mark.asyncio
async def test_timeout_is_bounded_and_sanitized():
    async def handler(_):
        await asyncio.sleep(1)
        return httpx.Response(200, json={})

    async with proxy_client(handler, replace(CONFIG, timeout_seconds=0.02)) as client:
        response = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 8, "method": "tools/list"}, headers={"authorization": bearer()})
    assert response.status_code == 502
    assert response.json()["error"]["message"] == "Downstream unavailable"


@pytest.mark.asyncio
async def test_origin_and_body_limits():
    def handler(_):
        pytest.fail("Rejected request reached downstream")

    async with proxy_client(handler, replace(CONFIG, max_body_bytes=100)) as client:
        response = await client.post("/mcp", json={}, headers={"authorization": bearer(), "origin": "https://untrusted.example"})
        assert response.status_code == 403
        response = await client.post("/mcp", content=b"x" * 101, headers={"authorization": bearer(), "content-type": "application/json"})
        assert response.status_code == 413


@pytest.mark.asyncio
async def test_response_limit():
    async with proxy_client(lambda _: httpx.Response(200, content=b"x" * 200), replace(CONFIG, max_body_bytes=100)) as client:
        response = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers={"authorization": bearer()})
    assert response.status_code == 502
    assert response.json()["error"]["message"] == "Downstream response too large"


@pytest.mark.asyncio
async def test_forwarded_notification_has_empty_http_202():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(202)

    async with proxy_client(handler) as client:
        response = await client.post("/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"}, headers={"authorization": bearer()})
    assert response.status_code == 202
    assert not response.content
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_json_only_proxy_rejects_sse_explicitly():
    def handler(_):
        return httpx.Response(200, content=b"data: {}\n\n", headers={"content-type": "text/event-stream"})

    async with proxy_client(handler) as client:
        response = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers={"authorization": bearer()})
    assert response.status_code == 502
    assert response.json()["error"]["message"] == "Downstream streaming is not supported"
