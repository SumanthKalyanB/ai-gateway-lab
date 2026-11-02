"""Task 1: validated tools over the official MCP SDK's stdio transport."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import Annotated, Any
from uuid import uuid4

from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

CustomerId = Annotated[str, StringConstraints(strict=True, pattern=r"^CUST-[0-9]{5}$", min_length=10, max_length=10)]
Reason = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=10)]


class CustomerInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    customer_id: CustomerId


class RefundInput(CustomerInput):
    amount: Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
    reason: Reason


# Fixture data only. No bank, payment processor, or personal records are accessed.
CUSTOMERS = {
    "CUST-00001": {"customer_id": "CUST-00001", "name": "Demo Customer", "status": "active"},
    "CUST-00002": {"customer_id": "CUST-00002", "name": "Sample Customer", "status": "active"},
}


def _invalid(message: str, data: Any = None) -> McpError:
    return McpError(types.ErrorData(code=types.INVALID_PARAMS, message=message, data=data))


def create_server() -> Server:
    server = Server("customer-tools", version="1.0.0")

    @server.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(name="get_customer_record", description="Read a fixture customer record.", inputSchema=CustomerInput.model_json_schema()),
            types.Tool(name="trigger_refund", description="Simulate a refund; no money moves.", inputSchema=RefundInput.model_json_schema()),
        ]

    async def call_tool(request: types.CallToolRequest) -> types.ServerResult:
        name = request.params.name
        models = {"get_customer_record": CustomerInput, "trigger_refund": RefundInput}
        if name not in models:
            raise _invalid("Unknown tool")
        try:
            parsed = models[name].model_validate(request.params.arguments or {})
        except ValidationError as exc:
            # Never echo invalid customer data or the submitted refund reason.
            details = [{"field": ".".join(map(str, item["loc"])), "type": item["type"]} for item in exc.errors()]
            raise _invalid("Invalid tool arguments", details) from None

        record = CUSTOMERS.get(parsed.customer_id)
        if record is None:
            result = types.CallToolResult(content=[types.TextContent(type="text", text="Customer not found")], isError=True)
        elif isinstance(parsed, RefundInput):
            payload = {"refund_id": f"DEMO-{uuid4().hex}", "customer_id": parsed.customer_id, "amount": parsed.amount, "status": "simulated"}
            result = types.CallToolResult(content=[types.TextContent(type="text", text=json.dumps(payload))], structuredContent=payload)
        else:
            result = types.CallToolResult(content=[types.TextContent(type="text", text=json.dumps(record))], structuredContent=dict(record))
        return types.ServerResult(result)

    # In SDK v1.26.0 @call_tool() converts validation exceptions to isError
    # results. Register directly so invalid parameters remain JSON-RPC -32602.
    server.request_handlers[types.CallToolRequest] = call_tool
    return server


async def serve() -> None:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(name)s: %(message)s", force=True)
    logging.getLogger(__name__).info("Starting customer-tools stdio server")
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    main()
