"""Task 4: atomic on-disk token budgets and a bounded primary/backup router.

Accounting: reserve before EACH attempt; finalize successful usage from the trusted
provider. 429 consumes zero tokens. An ambiguous failure keeps the full reservation.
A pending reservation survives a process crash for 75s (60s window + 15s grace).
Completed charges remain for 60s after completion: a conservative sliding window.

This adapter accepts only text messages. Its admission estimate is UTF-8 bytes plus
128 tokens of framing per message and 128 per request, plus max_tokens. Configure a
model-specific counter before using a provider whose framing/tokenizer can exceed
that bound. Provider-reported overages are recorded and treated as contract errors.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass, field
from typing import Annotated, Callable, Literal
from uuid import uuid4

import anyio
import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

log = logging.getLogger(__name__)


class BudgetExceeded(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = retry_after


class SQLiteTokenLimiter:
    """Independent connections + BEGIN IMMEDIATE serialize even separate workers."""

    def __init__(self, db_path: str | Path, limit: int = 50_000,
                 window: float = 60.0, clock: Callable[[], float] = time.time):
        if str(db_path) == ":memory:" or str(db_path).startswith("file:"):
            raise ValueError("An ordinary on-disk SQLite path is required")
        if limit <= 0 or window <= 0:
            raise ValueError("Positive limit/window required")
        self.db_path = str(db_path)
        self.limit, self.window, self.clock = limit, window, clock
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connection()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS token_charges (
                reservation TEXT PRIMARY KEY, tenant TEXT NOT NULL,
                tokens INTEGER NOT NULL CHECK(tokens >= 0),
                expires_at REAL NOT NULL, pending INTEGER NOT NULL)""")
            db.execute("CREATE INDEX IF NOT EXISTS tenant_expiry ON token_charges(tenant, expires_at)")

    def _connection(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=5, isolation_level=None)
        db.execute("PRAGMA busy_timeout=5000")
        return db

    def _reserve(self, tenant: str, tokens: int) -> str:
        if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens <= 0:
            raise ValueError("Positive integer token reservation required")
        now = self.clock()
        db = self._connection()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM token_charges WHERE expires_at <= ?", (now,))
            used = db.execute("SELECT COALESCE(SUM(tokens), 0) FROM token_charges WHERE tenant = ?", (tenant,)).fetchone()[0]
            if used + tokens > self.limit:
                expiry = db.execute("SELECT MIN(expires_at) FROM token_charges WHERE tenant = ? AND tokens > 0", (tenant,)).fetchone()[0]
                db.commit()
                raise BudgetExceeded(max(1, math.ceil((expiry or now + self.window) - now)))
            reservation = uuid4().hex
            db.execute("INSERT INTO token_charges VALUES (?, ?, ?, ?, 1)",
                       (reservation, tenant, tokens, now + self.window + 15.0))
            db.commit()
            return reservation
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    async def reserve(self, tenant: str, tokens: int) -> str:
        # Cancellation can leave a conservative reservation, never an uncharged call.
        return await asyncio.to_thread(self._reserve, tenant, tokens)

    def _settle(self, reservation: str, actual_tokens: int | None) -> None:
        if actual_tokens is not None and (type(actual_tokens) is not int or actual_tokens < 0):
            raise ValueError("Nonnegative integer usage required")
        now = self.clock()
        db = self._connection()
        try:
            db.execute("BEGIN IMMEDIATE")
            if actual_tokens is None:
                db.execute("UPDATE token_charges SET expires_at = ?, pending = 0 WHERE reservation = ? AND pending = 1",
                           (now + self.window, reservation))
            elif actual_tokens >= 0:
                db.execute("UPDATE token_charges SET tokens = ?, expires_at = ?, pending = 0 WHERE reservation = ? AND pending = 1",
                           (actual_tokens, now + self.window, reservation))
            else:
                raise ValueError("Negative token usage")
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    async def settle(self, reservation: str, actual_tokens: int | None = None) -> None:
        await asyncio.to_thread(self._settle, reservation, actual_tokens)

    async def used(self, tenant: str) -> int:
        def read() -> int:
            with closing(self._connection()) as db:
                return db.execute("SELECT COALESCE(SUM(tokens), 0) FROM token_charges WHERE tenant = ? AND expires_at > ?",
                                  (tenant, self.clock())).fetchone()[0]
        return await asyncio.to_thread(read)


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    role: Literal["system", "user", "assistant"]
    content: Annotated[str, Field(max_length=100_000)]

    @field_validator("content")
    @classmethod
    def valid_unicode(cls, value: str) -> str:
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("Valid Unicode text required") from None
        return value


class CompletionInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    messages: Annotated[list[Message], Field(min_length=1, max_length=100)]
    max_tokens: Annotated[int, Field(gt=0, le=16_384)] = 512
    stream: Literal[False] = False


def reservation_tokens(payload: CompletionInput) -> int:
    return 128 + sum(len(m.content.encode("utf-8")) + 128 for m in payload.messages) + payload.max_tokens


@dataclass(frozen=True)
class RouterSettings:
    db_path: str = "./data/rate_limits.sqlite3"
    tenant_keys: dict[str, str] = field(default_factory=dict, repr=False)
    primary_url: str = "http://127.0.0.1:9000/v1/chat/completions"
    backup_url: str = "http://127.0.0.1:9000/v1/chat/completions"
    primary_model: str = "primary-demo"
    backup_model: str = "backup-demo"
    primary_key: str = field(default="", repr=False)
    backup_key: str = field(default="", repr=False)
    primary_timeout: float = 3.0
    backup_timeout: float = 3.0
    tokens_per_minute: int = 50_000

    def __post_init__(self):
        if not self.tenant_keys or any(not isinstance(k, str) or len(k) < 16 or not isinstance(v, str) or not v for k, v in self.tenant_keys.items()):
            raise ValueError("Configure tenant API keys (at least 16 characters) mapped to tenant names")
        if any(httpx.URL(url).scheme not in ("http", "https") for url in (self.primary_url, self.backup_url)):
            raise ValueError("Provider URLs must use http or https")
        if not 0 < self.primary_timeout <= 3.0 or not 0 < self.backup_timeout <= 3.0:
            raise ValueError("Attempt timeouts must be in (0, 3] seconds")

    @classmethod
    def from_env(cls) -> RouterSettings:
        return cls(db_path=os.getenv("RATE_LIMIT_DB", "./data/rate_limits.sqlite3"),
                   tenant_keys=json.loads(os.environ["TENANT_API_KEYS_JSON"]),
                   primary_url=os.getenv("PRIMARY_URL", "http://127.0.0.1:9000/v1/chat/completions"),
                   backup_url=os.getenv("BACKUP_URL", "http://127.0.0.1:9000/v1/chat/completions"),
                   primary_model=os.getenv("PRIMARY_MODEL", "primary-demo"),
                   backup_model=os.getenv("BACKUP_MODEL", "backup-demo"),
                   primary_key=os.getenv("PRIMARY_API_KEY", ""),
                   backup_key=os.getenv("BACKUP_API_KEY", ""))


def gateway_error(status: int, code: str, message: str, request_id: str, **headers: str) -> JSONResponse:
    return JSONResponse(status_code=status,
                        content={"error": {"code": code, "message": message, "request_id": request_id}},
                        headers={"x-request-id": request_id, **headers})


class UpstreamFailure(Exception):
    def __init__(self, kind: str):
        self.kind = kind


async def _provider_request(client: httpx.AsyncClient, url: str, key: str,
                            model: str, payload: CompletionInput, deadline: float) -> tuple[int, dict | None]:
    headers = {"authorization": f"Bearer {key}"} if key else {}
    try:
        # HTTPX read timeouts alone do not cap total elapsed time across many reads.
        async with asyncio.timeout(deadline):
            async with client.stream("POST", url, headers=headers,
                                     json={**payload.model_dump(), "model": model},
                                     timeout=deadline) as response:
                if response.status_code != 200:
                    return response.status_code, None
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > 2_000_000:
                        raise UpstreamFailure("invalid_response")
                try:
                    parsed = json.loads(body)
                except (ValueError, UnicodeDecodeError):
                    raise UpstreamFailure("invalid_response") from None
                if not isinstance(parsed, dict):
                    raise UpstreamFailure("invalid_response")
                return 200, parsed
    except (TimeoutError, httpx.TimeoutException):
        raise UpstreamFailure("timeout") from None
    except httpx.HTTPError:
        raise UpstreamFailure("unavailable") from None


def validated_completion(data: dict) -> tuple[dict, int | None]:
    """Return only the public schema: never relay provider errors or debug fields."""
    choices = data.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise UpstreamFailure("invalid_response")
    choice = choices[0]
    message = choice.get("message") if isinstance(choice, dict) else None
    if not isinstance(message, dict) or not isinstance(message.get("content"), str):
        raise UpstreamFailure("invalid_response")
    finish = choice.get("finish_reason")
    if finish not in ("stop", "length", "content_filter"):
        raise UpstreamFailure("invalid_response")
    usage = data.get("usage")
    actual = None
    if usage is not None:
        if not isinstance(usage, dict):
            raise UpstreamFailure("invalid_response")
        values = [usage.get(k) for k in ("prompt_tokens", "completion_tokens", "total_tokens")]
        if any(type(n) is not int or n < 0 for n in values) or values[0] + values[1] != values[2]:
            raise UpstreamFailure("invalid_response")
        actual = values[2]
    result = {"object": "chat.completion", "choices": [{"index": 0,
              "message": {"role": "assistant", "content": message["content"]}, "finish_reason": finish}]}
    if actual is not None:
        result["usage"] = {k: usage[k] for k in ("prompt_tokens", "completion_tokens", "total_tokens")}
    return result, actual


def create_app(settings: RouterSettings | None = None, *, client: httpx.AsyncClient | None = None,
               limiter: SQLiteTokenLimiter | None = None) -> FastAPI:
    settings = settings or RouterSettings.from_env()
    limiter = limiter or SQLiteTokenLimiter(settings.db_path, settings.tokens_per_minute)
    owned_client = client is None
    provider = client or httpx.AsyncClient(follow_redirects=False, trust_env=False)
    # Multiple keys for the same tenant share one bucket; only hashes enter SQLite.
    credentials = [(hashlib.sha256(key.encode()).digest(), hashlib.sha256(tenant.encode()).hexdigest())
                   for key, tenant in settings.tenant_keys.items()]

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        if owned_client:
            await provider.aclose()

    app = FastAPI(title="Task 4 — model router", lifespan=lifespan)
    app.state.limiter = limiter

    @app.middleware("http")
    async def request_id(request: Request, call_next):
        request.state.request_id = uuid4().hex
        try:
            response = await call_next(request)
        except Exception:
            log.error("Gateway failure request_id=%s", request.state.request_id)
            return gateway_error(503, "gateway_unavailable", "The gateway is temporarily unavailable.", request.state.request_id)
        response.headers["x-request-id"] = request.state.request_id
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_input(request: Request, exc):
        return gateway_error(400, "invalid_request", "Request fields are invalid.", request.state.request_id)

    @app.post("/v1/chat/completions")
    async def complete(request: Request):
        rid = request.state.request_id
        authorization = request.headers.get("authorization", "")
        scheme, _, token = authorization.partition(" ")
        supplied = hashlib.sha256(token.encode()).digest()
        tenant = None
        for expected, candidate in credentials:
            if secrets.compare_digest(supplied, expected):
                tenant = candidate
        if scheme.lower() != "bearer" or tenant is None:
            return gateway_error(401, "unauthorized", "A valid tenant API key is required.", rid,
                                 **{"www-authenticate": "Bearer"})
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 1_000_000:
                return gateway_error(413, "request_too_large", "The request exceeds the gateway size limit.", rid)
        try:
            payload = CompletionInput.model_validate_json(body)
        except ValidationError:
            return gateway_error(400, "invalid_request", "Request fields are invalid.", rid)
        estimate = reservation_tokens(payload)
        if estimate > limiter.limit:
            return gateway_error(400, "token_budget_too_large", "The request token budget exceeds the tenant window limit.", rid)
        attempts = ((settings.primary_url, settings.primary_key, settings.primary_model, settings.primary_timeout),
                    (settings.backup_url, settings.backup_key, settings.backup_model, settings.backup_timeout))
        for index, (url, key, model, deadline) in enumerate(attempts):
            try:
                reservation = await limiter.reserve(tenant, estimate)
            except BudgetExceeded as exc:
                return gateway_error(429, "rate_limit_exceeded", "The tenant token budget is exhausted.", rid,
                                     **{"retry-after": str(exc.retry_after)})
            charged: int | None = None  # Keep full reservation on ambiguous failures/cancellation.
            failure = "unavailable"
            should_retry = False
            try:
                try:
                    status, data = await _provider_request(provider, url, key, model, payload, deadline)
                except UpstreamFailure as exc:
                    failure = exc.kind
                    should_retry = failure == "timeout"
                else:
                    if status == 429:
                        charged = 0
                        should_retry = True
                        failure = "rate_limited"
                    elif status != 200:
                        failure = "unavailable"
                    else:
                        try:
                            result, charged = validated_completion(data)
                        except UpstreamFailure:
                            failure = "invalid_response"
                        else:
                            if charged is not None and charged > estimate:
                                # Record even an overage so future admission cannot evade it.
                                failure = "invalid_response"
                            else:
                                result.update(id=f"chatcmpl-{rid}", created=int(time.time()), model=model)
                                return JSONResponse(result, headers={"x-gateway-provider": "primary" if index == 0 else "backup"})
            finally:
                # Starlette middleware cancels through AnyIO scopes, which deliver
                # cancellation again at each await. Shield the whole cleanup scope,
                # including the second await after a direct asyncio cancellation.
                # Keep the task shield too: Task.cancel() bypasses AnyIO scopes.
                with anyio.CancelScope(shield=True):
                    settlement = asyncio.create_task(limiter.settle(reservation, charged))
                    try:
                        await asyncio.shield(settlement)
                    except asyncio.CancelledError:
                        await settlement
                        raise
            if index == 0 and should_retry:
                continue
            status_code = 504 if failure == "timeout" else 503 if failure in ("rate_limited", "unavailable") else 502
            code = "upstream_timeout" if failure == "timeout" else "upstream_unavailable" if status_code == 503 else "invalid_upstream_response"
            return gateway_error(status_code, code, "The model provider could not complete the request.", rid)
        return gateway_error(503, "upstream_unavailable", "The model provider could not complete the request.", rid)

    return app
