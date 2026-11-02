import httpx
import pytest

from gateway_assessment.demo import app


@pytest.mark.asyncio
async def test_demo_provider_and_failure_modes():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://demo") as client:
        reply = await client.post("/v1/chat/completions", json={"max_tokens": 1000})
        assert reply.json()["choices"][0]["message"]["content"].startswith("Hello café")
        assert (await client.post("/primary429/v1/chat/completions")).status_code == 429
        stream = await client.post("/v1/chat/completions", json={"stream": True, "max_tokens": 1000})
        assert stream.headers["content-type"].startswith("text/event-stream")
        assert stream.text.endswith("data: [DONE]\n\n")
        tools = await client.post("/mcp", json={"jsonrpc": "2.0", "id": 0, "method": "tools/list"})
        assert tools.json()["id"] == 0
        assert tools.json()["result"]["tools"][1]["name"] == "admin_reset_key"
