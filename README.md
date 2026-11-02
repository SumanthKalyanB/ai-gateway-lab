# QuilrAI — AI Field Developer / Solutions Engineer Assessment

This project implements the four assessment tasks in Python. Each task runs separately so its behavior is easy to inspect. A bundled mock supplies the downstream MCP endpoint and LLM responses; no paid credentials are needed for the demo.

The customer records and refunds are fixtures. The code does not access real customers or move money.

## Setup and verification

Use Python 3.11 or later. The verified environment used Python 3.12.7.

Run these commands from this directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock
python -m pip install --no-deps -e .
python -m pytest -q
python scripts/smoke.py
```

`requirements.lock` pins the tested dependency set, including the official `mcp==1.26.0` SDK. `pyproject.toml` lists the direct dependencies. Installation needs internet access; tests and the local demo do not.

The smoke check starts the services on temporary loopback ports, checks authorization and redaction over real HTTP, exercises both fallback triggers, and stops the services afterward. Its SQLite database is a real temporary file. See `VERIFICATION.md` for the recorded results.

The GitHub Actions workflow in `.github/workflows/tests.yml` runs the same installation, automated suite, and HTTP integration checks on Python 3.12 for pushes and pull requests. View the current [GitHub Actions results](https://github.com/SumanthKalyanB/quilrai-ai-gateway-assessment/actions); `VERIFICATION.md` records the local verification environment.

## Files

| Task | Implementation | Tests |
| --- | --- | --- |
| 1: validated MCP server | `gateway_assessment/mcp_server.py` | `tests/test_mcp_server.py` |
| 2: MCP authorization proxy | `gateway_assessment/mcp_proxy.py` | `tests/test_mcp_proxy.py` |
| 3: streaming PII guardrail | `gateway_assessment/stream_gateway.py` | `tests/test_stream_gateway.py` |
| 4: token budget and fallback | `gateway_assessment/model_router.py` | `tests/test_model_router.py` |

`gateway_assessment/demo.py` supplies the local mock services. `scripts/smoke.py` checks their integration with the gateways.

## Task 1 — MCP server

Start the stdio server:

```bash
python -m gateway_assessment.mcp_server
```

The MCP client should launch this command as a subprocess. Startup and SDK logs go to stderr. Stdout contains only newline-delimited MCP JSON-RPC messages.

The two tools use strict Pydantic models:

- `get_customer_record`: `customer_id` must match `CUST-` followed by exactly five ASCII digits.
- `trigger_refund`: the same customer ID, a finite positive numeric amount, and a reason with at least ten characters after trimming whitespace.
- Both tools reject extra fields. Amounts reject booleans and numeric strings; JSON integers such as `2` are accepted as the numeric value `2.0`.

Valid fixture IDs are `CUST-00001` and `CUST-00002`. A valid ID that is absent from the fixtures returns a tool execution result with `isError: true`. Invalid tool arguments and unknown tool names return JSON-RPC `-32602` with the original request ID. Validation errors identify fields without echoing their submitted values.

The pinned SDK's convenience `call_tool` decorator converts exceptions into tool error results. To satisfy the assessment's explicit JSON-RPC error requirement, this server registers its `CallToolRequest` handler directly in the SDK's request handler table. Transport, initialization, tool discovery, and dispatch still use the official SDK. The subprocess test verifies this on the wire.

Example conversation, one JSON object per line. Send the initialized notification after receiving the initialization response:

```json
{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"manual-check","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":1,"method":"tools/list"}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"trigger_refund","arguments":{"customer_id":"CUST-00001","amount":12.5,"reason":"Duplicate payment"}}}
```

## Run the HTTP demos

In each terminal, activate the virtual environment and load the example settings from this directory:

```bash
source .venv/bin/activate
set -a
source .env.example
set +a
```

Start each of these in a separate terminal:

```bash
# Mock MCP and LLM upstreams
python -m gateway_assessment.demo serve --port 9000

# Task 2
uvicorn gateway_assessment.mcp_proxy:create_app --factory --host 127.0.0.1 --port 8010

# Task 3
uvicorn gateway_assessment.stream_gateway:create_app --factory --host 127.0.0.1 --port 8020

# Task 4
uvicorn gateway_assessment.model_router:create_app --factory --host 127.0.0.1 --port 8030
```

## Task 2 — MCP security proxy

The proxy accepts `POST /mcp`. It verifies the Bearer JWT's HS256 signature, issuer, audience, expiration, issued-at time, subject, and role. The role is read from verified claims, rather than trusting a decoded but unsigned payload.

For `tools/list`, the proxy forwards the original body and returns the downstream response bytes and status. Viewers can see admin tools in the list because transparent discovery is an explicit requirement. For `tools/call`, a viewer calling a name that starts with `admin_` gets the following response without a downstream call:

```json
{"jsonrpc":"2.0","id":0,"error":{"code":-32001,"message":"Unauthorized Tool Call"}}
```

An admin can invoke the same tool. Other authenticated methods pass through. The client Bearer token is never sent to the downstream service; a separately configured `MCP_DOWNSTREAM_TOKEN` can authenticate that hop.

Try the viewer restriction:

```bash
VIEWER_TOKEN=$(python -m gateway_assessment.demo token viewer)
curl -sS http://127.0.0.1:8010/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":0,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{}}}'
```

Use `token admin` to test the allowed path, or change the method to `tools/list` to check discovery.

The proxy rejects malformed envelopes, duplicate JSON keys, invalid parameter structures, and batch bodies. Request and response bodies are capped at 1 MiB and each downstream operation has a total deadline. Notifications receive no JSON-RPC response body. Browser origins are denied unless included in `MCP_ALLOWED_ORIGINS`.

This task is a lightweight HTTP/JSON-RPC reverse proxy to a JSON-response mock. It is not a full MCP transport adapter: downstream SSE, standalone GET event streams, resumability, and session termination are outside this implementation. Downstream SSE is rejected explicitly. Task 1 independently demonstrates the official stdio transport.

## Task 3 — streaming PII guardrail

The streaming endpoint is `POST /v1/chat/completions` on port 8020. It accepts one text-only choice. A configured gateway API key is required; the provider has its own optional credential.

```bash
curl -N http://127.0.0.1:8020/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"demo","messages":[{"role":"user","content":"Show the sample"}],"stream":true,"max_tokens":256}'
```

Combining the content deltas from the demo gives:

```text
Hello café. Contact [REDACTED] SSN: [REDACTED]. Card: [REDACTED]. End of demo.
```

The email's adjoining full stop is included in the masked token. This is a conservative choice.

The parser reconstructs SSE events from arbitrary byte fragments, including fragmented UTF-8, line endings, and multiline data fields. It extracts only text deltas and rebuilds outbound events. It does not pass through arbitrary provider fields that could carry unredacted content.

The redactor holds the current lexical token and a possible number run. This lets it recognize a sensitive value even when every character arrives in a separate delta. Safe text is emitted at lexical boundaries without waiting for a fixed-size output chunk or the complete response.

Supported patterns are unquoted email-like tokens containing `@`, canonical ASCII `123-45-6789` SSNs, and runs of at least thirteen digits separated by spaces or hyphens. Card detection deliberately omits a Luhn check, masking plausible numbers even if their checksum is invalid. Oversized lexical tokens and numeric candidates are replaced and discarded until the next boundary. Persistent redactor state is capped at 448 characters, and an SSE frame is capped at 64 KiB.

There is a latency tradeoff: a token cannot be emitted until it is safe to release. A delayed boundary can delay content, even though the buffer stays bounded. Quoted, obfuscated, and semantic PII need a broader detector; this implementation does not claim universal PII detection. Numeric masking can also produce false positives.

Before response headers, provider failures return sanitized HTTP errors. After streaming starts, failures produce a sanitized SSE error followed by `[DONE]`; pending text is discarded. Provider responses are closed on completion, failure, and client disconnect. Unsupported tool-call and reasoning deltas fail closed.

## Task 4 — rate limiting and fallback

The router accepts nonstreaming text completion requests on port 8030. Model selection belongs to the gateway, so the client does not supply a model or tenant ID.

```bash
curl -sS -i http://127.0.0.1:8030/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Hello"}],"max_tokens":256}'
```

The example configuration makes the primary return 429. A successful reply includes `x-gateway-provider: backup`. To exercise the 3000 ms primary deadline, set `PRIMARY_URL=http://127.0.0.1:9000/slow/v1/chat/completions` and restart the router. To exercise ordinary primary success, use `http://127.0.0.1:9000/v1/chat/completions`.

The limiter uses the on-disk database at `RATE_LIMIT_DB` and a default budget of 50,000 tokens per 60 seconds. Authentication maps API keys to tenants; multiple keys mapped to one tenant share a bucket. Only a hash of the tenant identifier is stored in SQLite.

Each admission uses a separate SQLite connection and a short `BEGIN IMMEDIATE` transaction. Expired charges are removed, active charges are summed, and a new reservation is inserted atomically. WAL mode and a busy timeout let independent gateway workers share the file without oversubscribing the budget. SQLite operations run outside the async event loop; no database transaction stays open during a provider call.

Accounting follows these rules:

1. Reserve a conservative prompt estimate plus `max_tokens` before every provider attempt.
2. On success, replace the reservation with validated usage from the trusted provider. Missing usage keeps the estimate. Reported usage above the reservation is still charged and produces a sanitized contract error.
3. A 429 releases that attempt's reservation. Timeouts and ambiguous failures keep the full estimate because the provider may have generated billable tokens.
4. A backup attempt needs its own reservation. If the primary's ambiguous charge exhausted the budget, the gateway returns its own 429 before contacting the backup.
5. Settled charges expire exactly 60 seconds after settlement. This charges requests at completion and conservatively retains their inflight budget. An abandoned pending reservation expires 75 seconds after admission, allowing a short crash-recovery grace period. There is no fixed-minute reset.

The prompt estimate is UTF-8 byte length plus 128 tokens of framing per message and 128 per request. This is an explicit adapter assumption for the text-only demo, not a universal tokenizer. A production adapter should supply a verified model-specific tokenizer and framing count. Output length is bounded by `max_tokens`; admission never trusts a client-supplied usage count.

The primary has a total 3000 ms deadline around headers and body consumption. On 429 or timeout, the router tries the backup once. Other HTTP failures do not trigger fallback. The primary operation is cancelled before backup begins. Both providers use separate configured credentials, and redirects are disabled.

Client cancellation also stops the provider attempt. Database settlement is shielded during cleanup so the request cannot exit before its token charge is finalized, including under the web framework's repeated cancellation behavior.

Gateway errors use one public shape and include a request ID:

```json
{"error":{"code":"upstream_timeout","message":"The model provider could not complete the request.","request_id":"..."}}
```

The gateway never returns raw provider error bodies or internal stack traces. Tenant exhaustion returns HTTP 429 and `Retry-After`; exhausted provider capacity returns 503, timeouts return 504, and malformed provider responses return 502.

## Deployment boundaries

The examples bind to loopback and use clearly marked demo credentials. For a real deployment, set provider URLs and credentials through deployment configuration, use TLS for network hops, and keep the SQLite file on a local durable volume shared by workers on one host. Multiple hosts need a shared transactional rate-limit store rather than copies of a SQLite file. These are separate assessment services; authentication, token accounting, and redaction can be composed into one gateway when their provider contracts match.

## References

The implementation uses these primary references:

- [Official Python MCP SDK, pinned v1.26.0](https://github.com/modelcontextprotocol/python-sdk/tree/v1.26.0) — SDK interfaces and error dispatch.
- [MCP tools and protocol errors](https://modelcontextprotocol.io/specification/2025-06-18/server/tools) and [MCP transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) — tool invocation and transport rules. The assessment's explicit wire-error requirement is enforced by tests.
- [JSON-RPC 2.0](https://www.jsonrpc.org/specification) — request IDs, notifications, and error envelopes.
- [Chat completion streaming events](https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events) and [Starlette responses](https://www.starlette.dev/responses/) — SSE delta format and response lifecycle.
- [SQLite transactions](https://www.sqlite.org/lang_transaction.html) — writer serialization and `BEGIN IMMEDIATE`.
- [Python async timeouts](https://docs.python.org/3.11/library/asyncio-task.html#timeouts) — total deadlines and cancellation behavior.
- [AnyIO cancellation and shielding](https://anyio.readthedocs.io/en/stable/cancellation.html) — protecting database cleanup from framework cancellation.
