"""Run a local HTTP integration check; all child processes are stopped on exit."""
from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time

import httpx
import jwt


ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main() -> None:
    processes: list[subprocess.Popen] = []
    log_handles = []
    with tempfile.TemporaryDirectory(prefix="assessment-smoke-") as directory:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        env["MCP_JWT_SECRET"] = secrets.token_hex(32)
        env["MCP_JWT_ISSUER"] = "assessment-auth"
        env["MCP_JWT_AUDIENCE"] = "mcp-gateway"
        env["GATEWAY_API_KEY"] = "demo-tenant-key-1234"
        env["TENANT_API_KEYS_JSON"] = json.dumps({env["GATEWAY_API_KEY"]: "demo-tenant"})
        mock_port = free_port()
        base = f"http://127.0.0.1:{mock_port}"
        env["MCP_DOWNSTREAM_URL"] = base + "/mcp"
        env["STREAM_PROVIDER_URL"] = base + "/v1/chat/completions"
        env["PRIMARY_URL"] = base + "/primary429/v1/chat/completions"
        env["BACKUP_URL"] = base + "/v1/chat/completions"
        env["RATE_LIMIT_DB"] = str(Path(directory) / "budget.sqlite3")

        def start(module: str, *, factory: bool = True, overrides: dict | None = None, port: int | None = None) -> str:
            port = port or free_port()
            log = open(Path(directory) / f"{port}.log", "w+")
            log_handles.append(log)
            command = [sys.executable, "-m", "uvicorn", module, "--host", "127.0.0.1", "--port", str(port)]
            if factory:
                command.append("--factory")
            child = subprocess.Popen(command, cwd=ROOT, env={**env, **(overrides or {})}, stdout=log, stderr=log)
            processes.append(child)
            url = f"http://127.0.0.1:{port}"
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise RuntimeError(f"Service {module} stopped at startup")
                try:
                    if httpx.get(url + "/openapi.json", timeout=0.3, trust_env=False).status_code == 200:
                        return url
                except httpx.HTTPError:
                    pass
                time.sleep(0.05)
            raise RuntimeError(f"Service {module} did not start")

        try:
            start("gateway_assessment.demo:app", factory=False, port=mock_port)
            proxy = start("gateway_assessment.mcp_proxy:create_app")
            streaming = start("gateway_assessment.stream_gateway:create_app")
            router = start("gateway_assessment.model_router:create_app")
            now = int(time.time())
            token = jwt.encode({"sub": "smoke", "role": "viewer", "iat": now, "exp": now + 120,
                                "iss": env["MCP_JWT_ISSUER"], "aud": env["MCP_JWT_AUDIENCE"]},
                               env["MCP_JWT_SECRET"], algorithm="HS256")
            with httpx.Client(timeout=12, trust_env=False) as client:
                headers = {"authorization": f"Bearer {token}"}
                listed = client.post(proxy + "/mcp", headers=headers,
                                     json={"jsonrpc": "2.0", "id": 0, "method": "tools/list"})
                assert listed.json()["id"] == 0 and len(listed.json()["result"]["tools"]) == 2
                denied = client.post(proxy + "/mcp", headers=headers,
                                     json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                           "params": {"name": "admin_reset_key"}})
                assert denied.json()["error"]["code"] == -32001
                print("PASS: MCP tool discovery and viewer authorization")
                headers = {"authorization": f"Bearer {env['GATEWAY_API_KEY']}"}
                streamed = client.post(streaming + "/v1/chat/completions", headers=headers,
                                       json={"model": "demo", "messages": [{"role": "user", "content": "demo"}],
                                             "stream": True, "max_tokens": 256})
                streamed.raise_for_status()
                output = ""
                for line in streamed.iter_lines():
                    if line.startswith("data: ") and line != "data: [DONE]":
                        value = json.loads(line[6:])
                        assert "error" not in value
                        output += value["choices"][0]["delta"].get("content", "")
                assert "alex@example.com" not in output and "123-45-6789" not in output
                assert "4111" not in output and output.count("[REDACTED]") == 3
                assert "café" in output and "End of demo." in output
                print("PASS: real HTTP SSE stream redacts all three PII examples")
                payload = {"messages": [{"role": "user", "content": "demo"}], "max_tokens": 256}
                routed = client.post(router + "/v1/chat/completions", headers=headers, json=payload)
                routed.raise_for_status()
                assert routed.headers["x-gateway-provider"] == "backup"
                assert routed.json()["usage"]["total_tokens"] > 0
                assert Path(env["RATE_LIMIT_DB"]).is_file()
                print("PASS: primary 429 triggers backup; SQLite file created")
                timeout_router = start("gateway_assessment.model_router:create_app", overrides={
                    "PRIMARY_URL": base + "/slow/v1/chat/completions",
                })
                begin = time.monotonic()
                routed = client.post(timeout_router + "/v1/chat/completions", headers=headers, json=payload)
                elapsed = time.monotonic() - begin
                routed.raise_for_status()
                assert routed.headers["x-gateway-provider"] == "backup" and 2.9 <= elapsed < 7
                print("PASS: primary 3000ms deadline triggers backup")
            print("All HTTP integration checks passed. Run pytest for stdio and edge cases.")
        except BaseException:
            for handle in log_handles:
                handle.flush()
                handle.seek(0)
                print(handle.read()[-4000:], file=sys.stderr)
            raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
            for process in reversed(processes):
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for handle in log_handles:
                handle.close()


if __name__ == "__main__":
    main()
