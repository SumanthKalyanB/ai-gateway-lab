"""Concurrency and wire-level tests for task 4; no live provider required."""
import asyncio
import hashlib
import sqlite3

import httpx
import pytest

from gateway_assessment.model_router import (
    BudgetExceeded, CompletionInput, Message, RouterSettings, SQLiteTokenLimiter,
    create_app, reservation_tokens,
)

KEY = "demo-tenant-key-1234"
TENANT = hashlib.sha256(b"demo-tenant").hexdigest()
HEADERS = {"Authorization": f"Bearer {KEY}"}
PAYLOAD = {"messages": [{"role": "user", "content": "Hello"}], "max_tokens": 10}


def completion(tokens=7):
    return {"choices": [{"message": {"role": "assistant", "content": "Hello back"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": tokens - 2, "completion_tokens": 2, "total_tokens": tokens}}


def settings(tmp_path, **overrides):
    return RouterSettings(db_path=str(tmp_path / "tokens.sqlite3"),
                          tenant_keys={KEY: "demo-tenant"},
                          primary_url="https://primary.test/completions",
                          backup_url="https://backup.test/completions", **overrides)


@pytest.mark.asyncio
async def test_reservations_are_atomic_across_independent_instances(tmp_path):
    path = tmp_path / "shared.sqlite3"
    limiters = [SQLiteTokenLimiter(path, limit=1000) for _ in range(8)]

    async def reserve(i):
        try:
            return await limiters[i % 8].reserve("tenant", 100)
        except BudgetExceeded:
            return None

    result = await asyncio.gather(*(reserve(i) for i in range(80)))
    assert sum(item is not None for item in result) == 10
    assert await limiters[0].used("tenant") == 1000
    # Verify the on-disk file and hashing are real, not an in-memory fallback.
    assert path.read_bytes().startswith(b"SQLite format 3")


@pytest.mark.asyncio
async def test_sliding_window_exact_boundary_and_pending_crash_grace(tmp_path):
    now = [100.0]
    limiter = SQLiteTokenLimiter(tmp_path / "tokens.db", limit=100, clock=lambda: now[0])
    reservation = await limiter.reserve("tenant", 100)
    await limiter.settle(reservation, 40)
    now[0] = 159.999
    assert await limiter.used("tenant") == 40
    now[0] = 160.0
    assert await limiter.used("tenant") == 0
    pending = await limiter.reserve("tenant", 100)
    now[0] = 220.0
    assert await limiter.used("tenant") == 100
    now[0] = 235.0
    assert await limiter.used("tenant") == 0
    assert pending


@pytest.mark.asyncio
async def test_settlement_is_idempotent_and_tenants_are_isolated(tmp_path):
    limiter = SQLiteTokenLimiter(tmp_path / "tokens.db", limit=100)
    first = await limiter.reserve("a", 100)
    await limiter.reserve("b", 100)
    await limiter.settle(first, 40)
    await limiter.settle(first, 1)  # duplicate callback cannot refund twice
    assert await limiter.used("a") == 40
    assert await limiter.used("b") == 100


@pytest.mark.asyncio
async def test_success_settles_actual_usage_and_does_not_expose_debug_fields(tmp_path):
    calls = []

    async def upstream(request):
        calls.append(request)
        result = completion()
        result["debug"] = "SECRET STACK TRACE"
        return httpx.Response(200, json=result)

    config = settings(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(config, client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
    assert response.status_code == 200
    assert response.headers["x-gateway-provider"] == "primary"
    assert response.json()["usage"]["total_tokens"] == 7
    assert "SECRET" not in response.text
    assert len(calls) == 1
    assert await app.state.limiter.used(TENANT) == 7
    with sqlite3.connect(config.db_path) as db:
        assert db.execute("SELECT tenant FROM token_charges").fetchone()[0] == TENANT


@pytest.mark.asyncio
async def test_429_falls_back_once_without_charging_rejected_attempt(tmp_path):
    hosts = []

    async def upstream(request):
        hosts.append(request.url.host)
        return httpx.Response(429, text="secret error") if request.url.host == "primary.test" else httpx.Response(200, json=completion())

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(settings(tmp_path), client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
    assert response.status_code == 200
    assert response.headers["x-gateway-provider"] == "backup"
    assert hosts == ["primary.test", "backup.test"]
    assert await app.state.limiter.used(TENANT) == 7


@pytest.mark.asyncio
async def test_total_deadline_cancels_primary_before_fallback(tmp_path):
    cancelled = asyncio.Event()
    hosts = []

    async def upstream(request):
        hosts.append(request.url.host)
        if request.url.host == "primary.test":
            try:
                await asyncio.sleep(30)
            finally:
                cancelled.set()
        return httpx.Response(200, json=completion())

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(settings(tmp_path, primary_timeout=0.02), client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
    assert response.status_code == 200
    assert cancelled.is_set()
    assert hosts == ["primary.test", "backup.test"]
    assert await app.state.limiter.used(TENANT) == reservation_tokens(CompletionInput.model_validate(PAYLOAD)) + 7


@pytest.mark.asyncio
async def test_budget_must_allow_both_ambiguous_primary_and_backup(tmp_path):
    calls = []

    async def upstream(request):
        calls.append(request.url.host)
        raise httpx.ReadTimeout("secret provider details")

    amount = reservation_tokens(CompletionInput.model_validate(PAYLOAD))
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(settings(tmp_path, tokens_per_minute=amount), client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
    assert response.status_code == 429
    assert response.json()["error"]["code"] == "rate_limit_exceeded"
    assert int(response.headers["retry-after"]) >= 1
    assert calls == ["primary.test"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 500, 503])
async def test_other_http_errors_do_not_trigger_fallback_or_leak_body(tmp_path, status):
    hosts = []

    async def upstream(request):
        hosts.append(request.url.host)
        return httpx.Response(status, text="Traceback: /secret/credentials")

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(settings(tmp_path), client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
    assert response.status_code == 503
    assert set(response.json()) == {"error"}
    assert set(response.json()["error"]) == {"code", "message", "request_id"}
    assert "secret" not in response.text and "Traceback" not in response.text
    assert hosts == ["primary.test"]


@pytest.mark.asyncio
async def test_both_timeouts_return_standardized_504(tmp_path):
    async def upstream(request):
        raise httpx.ReadTimeout("private stack")

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(settings(tmp_path), client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
    assert response.status_code == 504
    assert response.json()["error"]["code"] == "upstream_timeout"
    assert response.json()["error"]["request_id"] == response.headers["x-request-id"]
    assert await app.state.limiter.used(TENANT) == 2 * reservation_tokens(CompletionInput.model_validate(PAYLOAD))


@pytest.mark.asyncio
async def test_authentication_validation_and_request_size_do_not_call_provider(tmp_path):
    async def upstream(request):
        pytest.fail("Unvalidated request reached provider")

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(settings(tmp_path), client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            assert (await client.post("/v1/chat/completions", json=PAYLOAD)).status_code == 401
            for payload in ({**PAYLOAD, "tenant": "other"}, {**PAYLOAD, "stream": True}, {**PAYLOAD, "max_tokens": True}):
                assert (await client.post("/v1/chat/completions", headers=HEADERS, json=payload)).status_code == 400
            assert (await client.post("/v1/chat/completions", headers=HEADERS, content="x" * 1_000_001)).status_code == 413


@pytest.mark.asyncio
async def test_provider_missing_usage_keeps_conservative_reservation(tmp_path):
    async def upstream(request):
        data = completion()
        data.pop("usage")
        return httpx.Response(200, json=data)

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(settings(tmp_path), client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
    assert response.status_code == 200
    assert await app.state.limiter.used(TENANT) == reservation_tokens(CompletionInput.model_validate(PAYLOAD))


@pytest.mark.asyncio
async def test_provider_reported_overage_is_charged_and_sanitized(tmp_path):
    async def upstream(request):
        return httpx.Response(200, json=completion(100_000))

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(settings(tmp_path), client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
    assert response.status_code == 502
    assert await app.state.limiter.used(TENANT) == 100_000


def test_disk_and_key_requirements(tmp_path):
    with pytest.raises(ValueError):
        SQLiteTokenLimiter(":memory:")
    with pytest.raises(ValueError):
        RouterSettings(tenant_keys={"short": "tenant"})
    assert RouterSettings(tenant_keys={KEY: "tenant"}).primary_timeout == 3.0


@pytest.mark.asyncio
async def test_client_cancellation_finalizes_reservation_and_stops_provider(tmp_path):
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def upstream(request):
        started.set()
        try:
            await asyncio.sleep(30)
        finally:
            stopped.set()
        return httpx.Response(200, json=completion())

    config = settings(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as upstream_client:
        app = create_app(config, client=upstream_client)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
            operation = asyncio.create_task(client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD))
            await asyncio.wait_for(started.wait(), 1)
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation
    assert stopped.is_set()
    assert await app.state.limiter.used(TENANT) == reservation_tokens(CompletionInput.model_validate(PAYLOAD))
    with sqlite3.connect(config.db_path) as db:
        assert db.execute("SELECT pending FROM token_charges").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_cancel_scope_waits_for_delayed_sqlite_settlement(tmp_path):
    """ASGI middleware must not finish cancellation while SQLite is still pending.

    The event gate delays the actual on-disk settlement until the test releases
    it. AnyIO's task-state barrier lets the cancelled request run all its cleanup
    without depending on scheduler speed or a fixed sleep.
    """
    import anyio

    provider_started = anyio.Event()
    request_finished = anyio.Event()
    settlement_entered = anyio.Event()
    release_settlement = anyio.Event()
    settlement_finished = anyio.Event()
    settlement_wait_finished = anyio.Event()
    scopes = []

    class DelayedSettlementLimiter(SQLiteTokenLimiter):
        async def settle(self, reservation, actual_tokens=None):
            settlement_entered.set()
            try:
                await release_settlement.wait()
                await super().settle(reservation, actual_tokens)
                settlement_finished.set()
            finally:
                settlement_wait_finished.set()

    async def upstream(request):
        provider_started.set()
        return httpx.Response(200, json=completion())

    config = settings(tmp_path)
    limiter = DelayedSettlementLimiter(config.db_path)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as provider:
            app = create_app(config, client=provider, limiter=limiter)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://gateway") as client:
                async def run_request():
                    try:
                        with anyio.CancelScope() as scope:
                            scopes.append(scope)
                            await client.post("/v1/chat/completions", headers=HEADERS, json=PAYLOAD)
                    finally:
                        request_finished.set()

                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(run_request)
                    try:
                        with anyio.fail_after(5):
                            await provider_started.wait()
                            await settlement_entered.wait()
                            # Cancel while cleanup is actually awaiting settlement.
                            scopes[0].cancel()
                            await anyio.wait_all_tasks_blocked()
                            assert not settlement_wait_finished.is_set()
                            assert not request_finished.is_set()
                            assert not settlement_finished.is_set()
                            with sqlite3.connect(config.db_path) as db:
                                assert db.execute("SELECT pending FROM token_charges").fetchone()[0] == 1
                            release_settlement.set()
                            await request_finished.wait()
                    finally:
                        # Unblock shielded cleanup before the task group joins it,
                        # even if an assertion or the deadlock guard above fails.
                        release_settlement.set()
                assert settlement_finished.is_set()
    finally:
        release_settlement.set()
    assert await limiter.used(TENANT) == 7
    with sqlite3.connect(config.db_path) as db:
        assert db.execute("SELECT pending FROM token_charges").fetchone()[0] == 0
