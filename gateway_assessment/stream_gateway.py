"""Text-only SSE proxy with bounded, conservative streaming PII redaction.

Run: uvicorn gateway_assessment.stream_gateway:create_app --factory --port 8020

Scope: unquoted email-like tokens, canonical US SSNs, and 13+ digit runs
(with spaces/hyphens). No Luhn check is used: plausible card numbers are masked
conservatively. Quoted/obfuscated addresses and semantic PII need another layer.
The entire current token is held until its boundary; oversized tokens are masked
and discarded through that boundary. This deliberately trades occasional false
positives and one-token latency for bounded memory without leaking a token prefix.
"""
from __future__ import annotations

import hmac
import json
import os
import re
import time
import uuid
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from typing import Literal
from urllib.parse import urlsplit

import anyio
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

REDACTED = "[REDACTED]"
EMAIL_SYMBOLS = frozenset(".!#$%&'*+/=?^_`{|}~@-")
SSN = re.compile(r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)", re.ASCII)


class StreamingRedactor:
    """Incremental redactor; pending state never exceeds 320 + 128 characters.

    ``feed`` returns safe output immediately at lexical boundaries. Call ``finish``
    only on successful upstream completion; discard pending data on errors.
    Number runs over 19 digits are also masked, to avoid exposing a valid card
    embedded in a longer run. Oversized ordinary tokens are masked fail-closed.
    """

    def __init__(self, max_token: int = 320, max_number: int = 128):
        if max_token < 1 or max_number < 1:
            raise ValueError("Buffer bounds must be positive")
        self.max_token = max_token
        self.max_number = max_number
        self._token = ""
        self._token_overflow = False
        self._number = ""
        self._number_overflow = False
        self._finished = False

    @property
    def pending_characters(self) -> int:
        return len(self._token) + len(self._number)

    @staticmethod
    def _email_character(char: str) -> bool:
        return char.isalnum() or char in EMAIL_SYMBOLS

    def _end_number(self) -> str:
        if self._number_overflow:
            result = ""
        else:
            digits = sum("0" <= char <= "9" for char in self._number)
            # Preserve whitespace following a masked value.
            value = self._number.rstrip(" -")
            suffix = self._number[len(value):]
            result = REDACTED + suffix if digits >= 13 or SSN.search(value) else self._number
        self._number = ""
        self._number_overflow = False
        return result

    def _numbers(self, text: str) -> str:
        result: list[str] = []
        for char in text:
            is_digit = "0" <= char <= "9"
            in_run = bool(self._number) or self._number_overflow
            if is_digit or (in_run and char in " -"):
                if self._number_overflow:
                    continue
                if len(self._number) >= self.max_number:
                    self._number = ""
                    self._number_overflow = True
                    result.append(REDACTED)
                else:
                    self._number += char
            else:
                if in_run:
                    result.append(self._end_number())
                result.append(char)
        return "".join(result)

    def _end_token(self) -> str:
        if self._token_overflow:
            result = ""
        else:
            # Conservatively mask the whole lexical token containing @. This
            # covers punctuation adjoining an address without regex backtracking.
            result = REDACTED if "@" in self._token else self._token
        self._token = ""
        self._token_overflow = False
        return result

    def feed(self, text: str) -> str:
        if self._finished:
            raise RuntimeError("Cannot feed a finished redactor")
        result: list[str] = []
        for char in text:
            if self._email_character(char):
                if self._token_overflow:
                    continue
                if len(self._token) >= self.max_token:
                    self._token = ""
                    self._token_overflow = True
                    result.append(self._numbers(REDACTED))
                else:
                    self._token += char
            else:
                result.append(self._numbers(self._end_token() + char))
        return "".join(result)

    def finish(self) -> str:
        if self._finished:
            return ""
        self._finished = True
        return self._numbers(self._end_token()) + self._end_number()


class StreamProtocolError(Exception):
    """Upstream stream violates the bounded text-only contract."""


async def parse_sse(chunks: AsyncIterable[bytes], max_frame_bytes: int = 65_536) -> AsyncIterator[str]:
    """Parse SSE data fields across arbitrary UTF-8, LF, CRLF, and CR boundaries.

    Frame size counts all fields/comments, not just data, to bound adversarial
    input. Empty/comment-only events are ignored. Incomplete final events are
    discarded as required by SSE; the caller still requires a [DONE] sentinel.
    """
    line = bytearray()
    data: list[str] = []
    frame_bytes = 0
    after_cr = False

    def end_line() -> str | None:
        nonlocal frame_bytes
        try:
            value = line.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise StreamProtocolError("Invalid UTF-8") from exc
        line.clear()
        if not value:
            event = "\n".join(data) if data else None
            data.clear()
            frame_bytes = 0
            return event
        if value.startswith("data:"):
            field = value[5:]
            data.append(field[1:] if field.startswith(" ") else field)
        return None

    async for chunk in chunks:
        for byte in chunk:
            if after_cr:
                after_cr = False
                if byte == 10:
                    continue
            frame_bytes += 1
            if frame_bytes > max_frame_bytes:
                raise StreamProtocolError("SSE frame too large")
            if byte in (10, 13):
                event = end_line()
                after_cr = byte == 13
                if event is not None:
                    yield event
            else:
                line.append(byte)


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    role: Literal["system", "user", "assistant"]
    content: str = Field(max_length=100_000)


class CompletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    model: str = Field(min_length=1, max_length=200)
    messages: list[Message] = Field(min_length=1, max_length=100)
    stream: Literal[True] = True
    n: Literal[1] = 1
    max_tokens: int = Field(default=256, ge=1, le=8192)
    temperature: float = Field(default=0.7, ge=0, le=2, allow_inf_nan=False)


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


def _sse(value: dict | str) -> bytes:
    payload = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return f"data: {payload}\n\n".encode()


class _ProviderStreamingResponse(StreamingResponse):
    """Close the provider even if the downstream disappears before first body."""

    def __init__(self, *args, upstream: httpx.Response, **kwargs):
        super().__init__(*args, **kwargs)
        self.upstream = upstream

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            with anyio.CancelScope(shield=True):
                close_iterator = getattr(self.body_iterator, "aclose", None)
                if close_iterator is not None:
                    await close_iterator()
                await self.upstream.aclose()


def create_app(
    provider_url: str | None = None,
    provider_api_key: str | None = None,
    gateway_api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> FastAPI:
    """Create the gateway. Inject an AsyncClient for deterministic offline tests.

    STREAM_PROVIDER_URL defaults to the bundled demo provider; outbound secrets
    are never taken from the caller's Authorization header. GATEWAY_API_KEY is
    mandatory, avoiding an accidentally unauthenticated public provider proxy.
    """
    provider_url = provider_url or os.getenv("STREAM_PROVIDER_URL", "http://127.0.0.1:9000/v1/chat/completions")
    provider_api_key = provider_api_key if provider_api_key is not None else os.getenv("STREAM_PROVIDER_API_KEY")
    gateway_api_key = gateway_api_key if gateway_api_key is not None else os.getenv("GATEWAY_API_KEY")
    if not gateway_api_key:
        raise ValueError("GATEWAY_API_KEY must be configured")
    parsed = urlsplit(provider_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
        raise ValueError("Invalid configured provider URL")
    injected_client = client

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if injected_client is not None:
            app.state.client = injected_client
            yield
        else:
            async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0), follow_redirects=False) as owned:
                app.state.client = owned
                yield

    app = FastAPI(title="Streaming PII Gateway", lifespan=lifespan)
    # Also allow dependency-injected clients with ASGI test transports that do
    # not run lifespan automatically; the caller owns the injected client's life.
    if injected_client is not None:
        app.state.client = injected_client

    @app.post("/v1/chat/completions")
    async def complete(request: Request):
        authorization = request.headers.get("authorization", "")
        if not hmac.compare_digest(authorization.encode(), f"Bearer {gateway_api_key}".encode()):
            return _error(401, "unauthorized", "A valid gateway API key is required.")
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > 1_048_576:
                return _error(413, "request_too_large", "The request body exceeds the size limit.")
            body.extend(chunk)
        try:
            completion = CompletionRequest.model_validate_json(body)
        except ValidationError:
            return _error(422, "invalid_request", "Use a text-only, single-choice streaming completion request.")
        headers = {"accept": "text/event-stream"}
        if provider_api_key:
            headers["authorization"] = f"Bearer {provider_api_key}"
        upstream: httpx.Response | None = None
        try:
            outgoing = app.state.client.build_request("POST", provider_url, json=completion.model_dump(), headers=headers)
            upstream = await app.state.client.send(outgoing, stream=True)
        except httpx.TimeoutException:
            return _error(504, "upstream_timeout", "The provider did not respond in time.")
        except httpx.HTTPError:
            return _error(502, "upstream_unavailable", "The provider is temporarily unavailable.")
        if upstream.status_code != 200:
            await upstream.aclose()
            return _error(502, "upstream_unavailable", "The provider is temporarily unavailable.")
        if upstream.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "text/event-stream":
            await upstream.aclose()
            return _error(502, "invalid_upstream_response", "The provider returned an invalid stream.")
        response_id = "chatcmpl-" + uuid.uuid4().hex
        created = int(time.time())

        def envelope(delta: dict, finish_reason: str | None = None) -> dict:
            return {"id": response_id, "object": "chat.completion.chunk", "created": created,
                    "model": completion.model, "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]}

        async def generate() -> AsyncIterator[bytes]:
            redactor = StreamingRedactor()
            finished = False
            final_reason = "stop"
            try:
                yield _sse(envelope({"role": "assistant"}))
                async for event in parse_sse(upstream.aiter_bytes()):
                    if event == "[DONE]":
                        tail = redactor.finish()
                        if tail:
                            yield _sse(envelope({"content": tail}))
                        yield _sse(envelope({}, final_reason))
                        yield _sse("[DONE]")
                        return
                    try:
                        value = json.loads(event)
                        if not isinstance(value, dict) or "error" in value:
                            raise StreamProtocolError("Invalid completion event")
                        choices = value.get("choices")
                        if choices == [] and isinstance(value.get("usage"), dict):
                            continue
                        if not isinstance(choices, list) or len(choices) != 1:
                            raise StreamProtocolError("Expected one choice")
                        choice = choices[0]
                        if not isinstance(choice, dict) or choice.get("index") != 0:
                            raise StreamProtocolError("Invalid choice")
                        delta = choice.get("delta")
                        if not isinstance(delta, dict) or set(delta) - {"content", "role"}:
                            raise StreamProtocolError("Unsupported delta type")
                        if "role" in delta and delta["role"] != "assistant":
                            raise StreamProtocolError("Invalid role")
                        content = delta.get("content")
                        if content is not None and not isinstance(content, str):
                            raise StreamProtocolError("Expected text")
                        if finished and content:
                            raise StreamProtocolError("Content after finish")
                        if content:
                            content.encode("utf-8", errors="strict")
                            safe = redactor.feed(content)
                            if safe:
                                yield _sse(envelope({"content": safe}))
                        finish = choice.get("finish_reason")
                        if finish is not None:
                            if finish not in ("stop", "length"):
                                raise StreamProtocolError("Unsupported finish reason")
                            finished = True
                            final_reason = finish
                    except (ValueError, TypeError, KeyError) as exc:
                        raise StreamProtocolError("Invalid JSON event") from exc
                raise StreamProtocolError("Stream ended without DONE")
            except (httpx.HTTPError, StreamProtocolError):
                # Headers have already been sent. Send a protocol-level error;
                # never include the exception, upstream event, or pending PII.
                yield _sse({"error": {"code": "upstream_stream_error", "message": "The provider stream could not be completed."}})
                yield _sse("[DONE]")
            finally:
                with anyio.CancelScope(shield=True):
                    await upstream.aclose()

        return _ProviderStreamingResponse(generate(), upstream=upstream, media_type="text/event-stream", headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"})

    return app
