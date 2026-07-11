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



class MockStreamingOpenAIHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        chunks = [
            b'data: {"model":"mock-model","choices":[{"delta":{"content":"Hel"}}]}\n\n',
            b'data: {"choices":[{"delta":{"content":"lo"}}]}\n\n',
            b'data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":5,"total_tokens":16}}\n\n',
            b"data: [DONE]\n\n",
        ]
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()
        for chunk in chunks:
            self.wfile.write(chunk)
            self.wfile.flush()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


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


def test_bench_matrix_runs_cross_product(tmp_path: Path) -> None:
    init_git_repo(tmp_path)
    agentledger.init(tmp_path)

    matrix = tmp_path / "bench.toml"
    matrix.write_text(
        f"""
repeats = 2
allow_dirty = true

[[tasks]]
name = "greet"
prompt = "hello"
evals = ["test -f README.md"]

[[tasks]]
name = "env-check"

[[agents]]
name = "echo"
command = ["{sys.executable}", "-c", "import os; print('{{prompt}}', '{{task}}', os.environ.get('OPENAI_BASE_URL', 'no-proxy'))"]

[[providers]]
name = "mock"
upstream = "http://127.0.0.1:9/v1"
""",
        encoding="utf-8",
    )

    report = agentledger.bench(matrix=matrix, repo=tmp_path)
    assert report.cell_count == 4
    assert report.passed == 4
    assert report.failed == 0
    cells = report.data["cells"]
    assert {cell["agent"] for cell in cells} == {"echo@mock"}
    assert {cell["repeat"] for cell in cells} == {1, 2}

    compared = agentledger.compare(task="greet", root=tmp_path)
    assert compared.run_count == 2
    row = compared.data["runs"][0]
    assert row["agent"] == "echo@mock"
    assert row["eval_status"] == "passed"

    filtered = agentledger.bench(matrix=matrix, repo=tmp_path, task="env-check")
    assert filtered.cell_count == 2

    stdouts = [
        (run_dir / "stdout.txt").read_text(encoding="utf-8")
        for run_dir in (tmp_path / ".agentledger" / "runs").iterdir()
    ]
    greet_outputs = [stdout for stdout in stdouts if "hello greet" in stdout]
    assert greet_outputs
    assert all("http://127.0.0.1" in stdout for stdout in greet_outputs)  # proxy URL injected


class MockRateLimitedHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        payload = b'{"error": {"code": 429, "message": "rate limited"}}'
        self.send_response(429)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


def test_fail_on_llm_error_marks_run_failed(tmp_path: Path) -> None:
    init_git_repo(tmp_path)

    server = HTTPServer(("127.0.0.1", 0), MockRateLimitedHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    script = """
import json
import os
import urllib.request
import urllib.error

payload = json.dumps({"model": "mock", "messages": []}).encode("utf-8")
request = urllib.request.Request(
    os.environ["OPENAI_BASE_URL"] + "/chat/completions",
    data=payload,
    headers={"content-type": "application/json"},
    method="POST",
)
try:
    urllib.request.urlopen(request, timeout=5)
except urllib.error.HTTPError as exc:
    print("upstream said", exc.code)
"""

    try:
        strict = agentledger.run(
            task="llm-guard-strict",
            agent="custom",
            command=[sys.executable, "-c", script],
            repo=tmp_path,
            proxy_upstream=f"http://127.0.0.1:{server.server_port}/v1",
            fail_on_llm_error=True,
        )
        lenient = agentledger.run(
            task="llm-guard-lenient",
            agent="custom",
            command=[sys.executable, "-c", script],
            repo=tmp_path,
            proxy_upstream=f"http://127.0.0.1:{server.server_port}/v1",
        )
    finally:
        server.shutdown()
        thread.join(timeout=5)

    # The child exits 0 in both cases; only the strict run is failed by the guard.
    assert strict.data["llm_error_calls"] == 1
    assert strict.status == "failed"
    assert lenient.data["llm_error_calls"] == 1
    assert lenient.status == "passed"


def test_post_hoc_eval_updates_existing_run(tmp_path: Path) -> None:
    init_git_repo(tmp_path)
    agentledger.init(tmp_path)

    result = agentledger.run(
        task="smoke",
        agent="custom",
        command=[sys.executable, "-c", "print('ok')"],
        repo=tmp_path,
    )
    assert result.data["evals"] == []

    updated = agentledger.eval(
        run_id=result.id,
        tests=["test -f README.md"],
        root=tmp_path,
    )
    assert updated.id == result.id
    assert updated.status == "passed"
    assert len(updated.data["evals"]) == 1

    report = agentledger.compare(task="smoke", root=tmp_path)
    assert report.run_count == 1  # re-eval must not duplicate the run
    assert report.data["runs"][0]["eval_status"] == "passed"

    failed = agentledger.eval(
        run_id=result.id,
        tests=["test -f missing-file.txt"],
        root=tmp_path,
    )
    assert failed.status == "failed"
    report = agentledger.compare(task="smoke", root=tmp_path)
    assert report.run_count == 1
    assert report.data["runs"][0]["eval_status"] == "failed"


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


def test_run_with_streaming_proxy_records_ttft(tmp_path: Path) -> None:
    init_git_repo(tmp_path)

    server = HTTPServer(("127.0.0.1", 0), MockStreamingOpenAIHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    script = """
import json
import os
import urllib.request

payload = json.dumps({
    "model": "mock-model",
    "stream": True,
    "messages": [{"role": "user", "content": "hello"}],
}).encode("utf-8")
request = urllib.request.Request(
    os.environ["OPENAI_BASE_URL"] + "/chat/completions",
    data=payload,
    headers={"content-type": "application/json"},
    method="POST",
)
body = urllib.request.urlopen(request, timeout=5).read().decode("utf-8")
assert "data: [DONE]" in body, body
print(body)
"""

    try:
        result = agentledger.run(
            task="proxy-stream",
            agent="custom",
            command=[sys.executable, "-c", script],
            repo=tmp_path,
            proxy_upstream=f"http://127.0.0.1:{server.server_port}/v1",
        )
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert result.status == "passed"
    assert "Hel" in result.data["stdout_preview"]

    calls_path = tmp_path / ".agentledger" / "llm_calls.ndjson"
    record = json.loads(calls_path.read_text(encoding="utf-8").strip().splitlines()[0])
    assert record["run_id"] == result.id
    assert record["source_precision"] == "exact"
    assert record["metrics"]["total_tokens"] == 16
    assert record["metrics"]["ttft_ms"] is not None

    report = agentledger.compare(task="proxy-stream", root=tmp_path)
    row = report.data["runs"][0]
    assert row["token_total"] == 16
    assert row["llm_metrics_precision"] == "exact"
