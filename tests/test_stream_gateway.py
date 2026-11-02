import json

import httpx
import pytest

from gateway_assessment.stream_gateway import (
    REDACTED, StreamProtocolError, StreamingRedactor, create_app, parse_sse,
)


async def byte_chunks(data, width=1):
    for offset in range(0, len(data), width):
        yield data[offset:offset + width]


def redact(parts):
    guard = StreamingRedactor()
    return "".join(guard.feed(part) for part in parts) + guard.finish()


@pytest.mark.parametrize("secret", [
    "alex@example.com", "élise@exämple.fr", "123-45-6789",
    "4111111111111111", "4111 1111 1111 1111", "4111-1111-1111-1111",
    "1234567890123", "12345678901234567890",
])
def test_every_possible_split_redacts(secret):
    text = f"Before {secret} after."
    for split in range(len(text) + 1):
        assert redact([text[:split], text[split:]]) == f"Before {REDACTED} after."
    assert redact(list(text)) == f"Before {REDACTED} after."


def test_redacts_multiple_values_and_preserves_normal_text():
    text = "Call 123-45-6789, email me@example.com; card 4242 4242 4242 4242. Thanks!"
    assert redact(list(text)) == f"Call {REDACTED}, email {REDACTED}; card {REDACTED}. Thanks!"
    normal = "Hello, this costs 12.50 and order 1234 is ready.\nUseful details follow."
    assert redact(list(normal)) == normal


def test_bounded_memory_and_fail_closed_oversized_token():
    guard = StreamingRedactor()
    output = []
    for _ in range(10_000):
        output.append(guard.feed("a" * 100))
        assert guard.pending_characters <= 448
    output.append(guard.feed("@example.com end "))
    output.append(guard.finish())
    assert "".join(output) == "[REDACTED] end "


def test_bounded_number_run():
    guard = StreamingRedactor()
    output = []
    for _ in range(1000):
        output.append(guard.feed("1234 "))
        assert guard.pending_characters <= 448
    output.append(guard.feed("end "))
    output.append(guard.finish())
    assert "".join(output) == "[REDACTED]end "


def test_progress_before_end_of_stream():
    guard = StreamingRedactor()
    assert guard.feed("Hello ") == "Hello "
    assert guard.feed("email alex@") == "email "
    assert guard.feed("example.com ") == "[REDACTED] "
    assert guard.finish() == ""
    assert guard.finish() == ""
    with pytest.raises(RuntimeError):
        guard.feed("later")


@pytest.mark.asyncio
@pytest.mark.parametrize("separator", [b"\n", b"\r\n", b"\r"])
async def test_sse_utf8_fragmentation_multiline_comments(separator):
    data = separator.join([b": heartbeat", b'data: {"message":', 'data: "café"}'.encode(), b"", b"data: [DONE]", b"", b""])
    events = [item async for item in parse_sse(byte_chunks(data))]
    assert json.loads(events[0]) == {"message": "café"}
    assert events[1] == "[DONE]"


@pytest.mark.asyncio
async def test_sse_frame_bound_including_comments_and_bad_utf8():
    with pytest.raises(StreamProtocolError):
        _ = [item async for item in parse_sse(byte_chunks(b":" + b"x" * 100), max_frame_bytes=32)]
    with pytest.raises(StreamProtocolError):
        _ = [item async for item in parse_sse(byte_chunks(b"data: \xff\n\n"))]
    assert [item async for item in parse_sse(byte_chunks(b"data: incomplete"))] == []


class FragmentedStream(httpx.AsyncByteStream):
    def __init__(self, body):
        self.body = body
        self.closed = False

    async def __aiter__(self):
        async for part in byte_chunks(self.body, 3):
            yield part

    async def aclose(self):
        self.closed = True


def event(content, **extra):
    return {"choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}], **extra}


def frames(*events, done=True):
    body = b"".join(b"data: " + json.dumps(value, ensure_ascii=False).encode() + b"\n\n" for value in events)
    return body + (b"data: [DONE]\n\n" if done else b"")


def content_of(body):
    events = [json.loads(frame[6:]) for frame in body.split("\n\n") if frame.startswith("data: {")]
    text = "".join(value["choices"][0]["delta"].get("content", "") for value in events if "choices" in value)
    return text, events


@pytest.mark.asyncio
async def test_endpoint_fragmented_stream_and_credentials():
    upstream_stream = FragmentedStream(frames(event("Hello café "), event("alex@"), event("example.com and 123-"), event("45-6789.")))
    seen = []

    def upstream(request):
        seen.append(request)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=upstream_stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as provider:
        app = create_app("https://provider.test/v1/chat/completions", "provider-secret", "client-secret", provider)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers={"Authorization": "Bearer client-secret"}, json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]})
    assert response.status_code == 200
    assert response.headers["x-accel-buffering"] == "no"
    assert content_of(response.text)[0] == "Hello café [REDACTED] and [REDACTED]."
    assert response.text.endswith("data: [DONE]\n\n")
    assert upstream_stream.closed
    assert seen[0].headers["authorization"] == "Bearer provider-secret"
    assert b'"stream":true' in seen[0].content


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [
    frames(event("pending@example.com"), done=False),
    frames({"error": {"message": "secret internal stack trace"}}),
    frames({"choices": [{"index": 0, "delta": {"tool_calls": [{"arguments": "secret"}]}}]}),
    b"data: not json\n\n",
    b"data: " + json.dumps(event("\ud800")).encode() + b"\n\n",
    b"data: " + b"x" * 70_000 + b"\n\n",
])
async def test_midstream_errors_are_sanitized_and_pending_pii_discarded(body):
    stream = FragmentedStream(body)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))) as provider:
        app = create_app(gateway_api_key="test", client=provider)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers={"authorization": "Bearer test"}, json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]})
    text, events = content_of(response.text)
    assert text == ""
    assert events[-1]["error"]["code"] == "upstream_stream_error"
    assert "secret" not in response.text
    assert "pending@example.com" not in response.text
    assert stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,status", [("timeout", 504), ("failure", 502), ("wrong-type", 502)])
async def test_preheader_upstream_errors(kind, status):
    def handler(request):
        if kind == "timeout":
            raise httpx.ReadTimeout("raw stack and API key", request=request)
        if kind == "failure":
            return httpx.Response(500, text="raw internal stack")
        return httpx.Response(200, text="raw internal stack")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as provider:
        app = create_app(gateway_api_key="test", client=provider)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers={"authorization": "Bearer test"}, json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]})
    assert response.status_code == status
    assert "raw" not in response.text
    assert "error" in response.json()


@pytest.mark.asyncio
async def test_authorization_and_unsupported_modes_never_call_provider():
    def handler(_):
        pytest.fail("Provider must not be called")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as provider:
        app = create_app(gateway_api_key="test", client=provider)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            base = {"model": "mock", "messages": [{"role": "user", "content": "hi"}]}
            assert (await client.post("/v1/chat/completions", json=base)).status_code == 401
            for fields in ({"tools": []}, {"stream": False}, {"n": 2}, {"max_tokens": True}, {"messages": [{"role": "user", "content": [{"image_url": "x"}]}]}):
                response = await client.post("/v1/chat/completions", headers={"authorization": "Bearer test"}, json={**base, **fields})
                assert response.status_code == 422
            response = await client.post("/v1/chat/completions", headers={"authorization": "Bearer test"}, content=b"x" * 1_048_577)
            assert response.status_code == 413


def test_configuration_requires_auth(monkeypatch):
    monkeypatch.delenv("GATEWAY_API_KEY", raising=False)
    with pytest.raises(ValueError, match="GATEWAY_API_KEY"):
        create_app()
    with pytest.raises(ValueError, match="provider URL"):
        create_app(provider_url="file:///etc/passwd", gateway_api_key="test")


@pytest.mark.asyncio
async def test_safe_text_is_sent_before_next_provider_chunk():
    import asyncio

    safe_text_sent = asyncio.Event()

    class ResponsiveStream(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            yield frames(event("Hello "), done=False)
            # If the proxy buffers 4096 bytes (or the entire response), this
            # cannot complete: the caller has to receive safe text first.
            await safe_text_sent.wait()
            yield b"data: [DONE]\n\n"

        async def aclose(self):
            self.closed = True

    stream = ResponsiveStream()
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))) as provider:
        app = create_app(gateway_api_key="test", client=provider)
        body = json.dumps({"model": "mock", "messages": [{"role": "user", "content": "hi"}]}).encode()
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"}, "http_version": "1.1", "method": "POST", "scheme": "http", "path": "/v1/chat/completions", "raw_path": b"/v1/chat/completions", "query_string": b"", "root_path": "", "server": ("test", 80), "client": ("client", 1234), "headers": [(b"content-type", b"application/json"), (b"authorization", b"Bearer test")]}

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message):
            if message["type"] == "http.response.body" and b'"content":"Hello "' in message.get("body", b""):
                safe_text_sent.set()

        await asyncio.wait_for(app(scope, receive, send), timeout=1.0)
    assert safe_text_sent.is_set()
    assert stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("disconnect_at", ["http.response.start", "http.response.body"])
async def test_downstream_disconnect_closes_provider(disconnect_at):
    from starlette.requests import ClientDisconnect

    stream = FragmentedStream(frames(event("Hello ")))
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))) as provider:
        app = create_app(gateway_api_key="test", client=provider)
        body = json.dumps({"model": "mock", "messages": [{"role": "user", "content": "hi"}]}).encode()
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"}, "http_version": "1.1", "method": "POST", "scheme": "http", "path": "/v1/chat/completions", "raw_path": b"/v1/chat/completions", "query_string": b"", "root_path": "", "server": ("test", 80), "client": ("client", 1234), "headers": [(b"content-type", b"application/json"), (b"authorization", b"Bearer test")]}

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message):
            if message["type"] == disconnect_at:
                raise OSError("Client disconnected")

        with pytest.raises(ClientDisconnect):
            await app(scope, receive, send)
    assert stream.closed


@pytest.mark.asyncio
async def test_preserves_length_finish_reason():
    final = {"choices": [{"index": 0, "delta": {}, "finish_reason": "length"}]}
    stream = FragmentedStream(frames(event("Hello "), final))
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))) as provider:
        app = create_app(gateway_api_key="test", client=provider)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            response = await client.post("/v1/chat/completions", headers={"authorization": "Bearer test"}, json={"model": "mock", "messages": [{"role": "user", "content": "hi"}]})
    assert content_of(response.text)[1][-1]["choices"][0]["finish_reason"] == "length"
