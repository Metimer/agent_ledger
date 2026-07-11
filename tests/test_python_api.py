from __future__ import annotations

import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import agentledger


def init_git_repo(path: Path) -> None:
    subprocess.run(["git", "init"], cwd=path, check=True, stdout=subprocess.PIPE)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=path, check=True)
    (path / "README.md").write_text("# fixture\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=path, check=True, stdout=subprocess.PIPE)



class MockOpenAICompatibleHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", "0"))
        request_body = json.loads(self.rfile.read(length).decode("utf-8"))
        self.server.requests.append((self.path, request_body))  # type: ignore[attr-defined]
        response = {
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "model": request_body.get("model", "mock-model"),
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 11,
                "completion_tokens": 5,
                "total_tokens": 16,
            },
        }
        payload = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


def test_init_run_compare_export(tmp_path: Path) -> None:
    init_git_repo(tmp_path)

    assert "Initialized AgentLedger" in agentledger.init(tmp_path)
    result = agentledger.run(
        task="smoke",
        agent="custom",
        command=[sys.executable, "-c", "import os; print(os.environ['AGENTLEDGER_RUN_ID'])"],
        repo=tmp_path,
    )

    assert result.status == "passed"
    assert result.data["stdout_preview"].strip() == result.id

    llm_calls = tmp_path / ".agentledger" / "llm_calls.ndjson"
    llm_calls.write_text(
        json.dumps(
            {
                "record_type": "llm_call",
                "schema_version": 1,
                "run_id": result.id,
                "duration_ms": 1000,
                "metrics": {
                    "input_tokens": 7,
                    "output_tokens": 3,
                    "total_tokens": 10,
                    "cost_usd": 0.001,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = agentledger.compare(task="smoke", root=tmp_path)
    assert report.run_count == 1
    row = report.data["runs"][0]
    assert row["token_total"] == 10
    assert row["llm_call_count"] == 1
    assert row["llm_metrics_precision"] == "exact"
    assert "| smoke |" in report.to_markdown()

    output = tmp_path / "runs.csv"
    assert "Exported CSV" in agentledger.export(format="csv", output=output, root=tmp_path)
    assert output.exists()


def test_doctor(tmp_path: Path) -> None:
    text = agentledger.doctor(tmp_path)
    assert "AgentLedger" in text
    assert "agents:" in text


def test_proxy_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "agentledger", "proxy", "--help"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    assert "--upstream" in result.stdout
    assert "--record-bodies" in result.stdout

    run_help = subprocess.run(
        [sys.executable, "-m", "agentledger", "run", "--help"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    assert "--proxy-upstream" in run_help.stdout
    assert "--proxy-record-bodies" in run_help.stdout


def test_run_with_integrated_proxy_records_llm_metrics(tmp_path: Path) -> None:
    init_git_repo(tmp_path)

    server = HTTPServer(("127.0.0.1", 0), MockOpenAICompatibleHandler)
    server.requests = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    script = """
import json
import os
import urllib.request

payload = json.dumps({
    "model": "mock-model",
    "messages": [{"role": "user", "content": "hello"}],
}).encode("utf-8")
request = urllib.request.Request(
    os.environ["OPENAI_BASE_URL"] + "/chat/completions",
    data=payload,
    headers={"content-type": "application/json"},
    method="POST",
)
print(urllib.request.urlopen(request, timeout=5).read().decode("utf-8"))
"""

    try:
        result = agentledger.run(
            task="proxy-smoke",
            agent="custom",
            command=[sys.executable, "-c", script],
            repo=tmp_path,
            proxy_upstream=f"http://127.0.0.1:{server.server_port}/v1",
        )
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert result.status == "passed"
    assert server.requests[0][0] == "/v1/chat/completions"  # type: ignore[attr-defined]

    report = agentledger.compare(task="proxy-smoke", root=tmp_path)
    row = report.data["runs"][0]
    assert row["token_total"] == 16
    assert row["llm_call_count"] == 1
    assert row["llm_metrics_precision"] == "exact"
