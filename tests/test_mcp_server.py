from __future__ import annotations

import asyncio
import json
import sys
from contextlib import asynccontextmanager

import pytest
from pydantic import ValidationError

from gateway_assessment.mcp_server import CustomerInput, RefundInput


@pytest.mark.parametrize("value", ["cust-00001", "CUST-1234", "CUST-123456", "CUST-１２３４５", "CUST-00001\n", 12345, None])
def test_customer_schema_rejects_invalid_ids(value):
    with pytest.raises(ValidationError):
        CustomerInput.model_validate({"customer_id": value})


@pytest.mark.parametrize("amount", [0, -1, True, "1.25", float("inf"), float("nan"), None])
def test_amount_is_a_positive_finite_number(amount):
    with pytest.raises(ValidationError):
        RefundInput.model_validate({"customer_id": "CUST-00001", "amount": amount, "reason": "Duplicate payment"})


def test_refund_accepts_numeric_amount_and_normalizes_reason():
    parsed = RefundInput.model_validate({"customer_id": "CUST-00001", "amount": 2, "reason": "  Duplicate payment  "})
    assert parsed.amount == 2.0
    assert parsed.reason == "Duplicate payment"
    with pytest.raises(ValidationError):
        RefundInput.model_validate({"customer_id": "CUST-00001", "amount": 2, "reason": "          "})
    with pytest.raises(ValidationError):
        CustomerInput.model_validate({"customer_id": "CUST-00001", "unexpected": 1})


@asynccontextmanager
async def running_server():
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "gateway_assessment.mcp_server",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    lines = []

    async def send(message, expect_response=True):
        process.stdin.write(json.dumps(message).encode() + b"\n")
        await process.stdin.drain()
        if expect_response:
            line = await asyncio.wait_for(process.stdout.readline(), timeout=10)
            assert line, "Server closed stdout unexpectedly"
            lines.append(line)
            return json.loads(line)

    try:
        initialized = await send({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {
            "protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "assessment-test", "version": "1.0"},
        }})
        assert initialized["id"] == 0
        assert "result" in initialized
        await send({"jsonrpc": "2.0", "method": "notifications/initialized"}, False)
        yield send, lines
    finally:
        process.stdin.close()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
        trailing = await process.stdout.read()
        assert not trailing, f"Unexpected stdout: {trailing!r}"
        stderr = await process.stderr.read()
        assert b"Starting customer-tools stdio server" in stderr
        assert process.returncode == 0


@pytest.mark.asyncio
async def test_actual_stdio_protocol_and_error_mapping():
    async with running_server() as (send, lines):
        listing = await send({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        tools = listing["result"]["tools"]
        assert {tool["name"] for tool in tools} == {"get_customer_record", "trigger_refund"}
        assert all(tool["inputSchema"]["additionalProperties"] is False for tool in tools)
        found = await send({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "get_customer_record", "arguments": {"customer_id": "CUST-00001"}}})
        assert found["result"]["structuredContent"]["customer_id"] == "CUST-00001"
        invalid_arguments = [
            {"customer_id": "bad", "amount": 1.5, "reason": "Duplicate payment"},
            {"customer_id": "CUST-00001", "amount": 0, "reason": "Duplicate payment"},
            {"customer_id": "CUST-00001", "amount": True, "reason": "Duplicate payment"},
            {"customer_id": "CUST-00001", "amount": "1.5", "reason": "Duplicate payment"},
            {"customer_id": "CUST-00001", "amount": 1.5, "reason": "short"},
            {"customer_id": "CUST-00001", "amount": 1.5, "reason": "Duplicate payment", "extra": 1},
            {},
        ]
        for i, arguments in enumerate(invalid_arguments, 10):
            result = await send({"jsonrpc": "2.0", "id": i, "method": "tools/call", "params": {"name": "trigger_refund", "arguments": arguments}})
            assert result["id"] == i
            assert result["error"]["code"] == -32602
            assert "result" not in result
        refund = await send({"jsonrpc": "2.0", "id": "refund", "method": "tools/call", "params": {"name": "trigger_refund", "arguments": {"customer_id": "CUST-00001", "amount": 12.5, "reason": "Duplicate payment"}}})
        assert refund["result"]["structuredContent"]["status"] == "simulated"
        missing = await send({"jsonrpc": "2.0", "id": "missing", "method": "tools/call", "params": {"name": "get_customer_record", "arguments": {"customer_id": "CUST-99999"}}})
        assert missing["result"]["isError"] is True
        unknown = await send({"jsonrpc": "2.0", "id": "unknown", "method": "tools/call", "params": {"name": "no_such_tool", "arguments": {}}})
        assert unknown["error"]["code"] == -32602
        for line in lines:
            assert json.loads(line)["jsonrpc"] == "2.0"
