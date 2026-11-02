# Verification record

Verified on November 2, 2026 using Python 3.12.7. No external LLM or payment service was called.

## Automated suite

```text
python -m pytest -q
88 passed in 1.60s
```

Coverage includes:

- Task 1: a real stdio subprocess, MCP initialization and tool discovery, valid calls, JSON-RPC `-32602` for invalid arguments, and separation of stdout and stderr.
- Task 2: unmodified tool discovery responses, viewer/admin authorization, no downstream invocation on denied calls, JWT rejection, malformed JSON, notifications, body limits, and bounded downstream timeouts.
- Task 3: all two-part splits of representative PII strings, character-by-character input, fragmented UTF-8/SSE, one-million-character adversarial tokens, bounded state, prompt delivery of safe text, sanitized stream errors, and provider cleanup on disconnect.
- Task 4: 80 concurrent reservation attempts through eight independently connected SQLite limiters, exact expiry boundaries, tenant isolation, actual usage settlement, conservative failure charges, primary cancellation before fallback, both fallback triggers, sanitized errors, and client cancellation.

## Live HTTP integration

```text
python scripts/smoke.py
PASS: MCP tool discovery and viewer authorization
PASS: real HTTP SSE stream redacts all three PII examples
PASS: primary 429 triggers backup; SQLite file created
PASS: primary 3000ms deadline triggers backup
All HTTP integration checks passed. Run pytest for stdio and edge cases.
```

This check used real local HTTP sockets and separate Uvicorn processes, a deterministic mock provider, and a temporary on-disk SQLite database. It exercised the default 3-second deadline in addition to the accelerated timeout tests in the unit suite.

The package was also built and installed successfully as an editable Python package with dependencies supplied from `requirements.lock`.

These results verify the implemented assessment behavior. They are not measurements of production load capacity or universal PII detection; supported formats and deployment boundaries are documented in `README.md`.
