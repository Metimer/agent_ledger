from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import agentledger


def init_git_repo(path: Path) -> None:
    subprocess.run(["git", "init"], cwd=path, check=True, stdout=subprocess.PIPE)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=path, check=True)
    (path / "README.md").write_text("# fixture\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=path, check=True, stdout=subprocess.PIPE)


def test_init_run_compare_export(tmp_path: Path) -> None:
    init_git_repo(tmp_path)

    assert "Initialized AgentLedger" in agentledger.init(tmp_path)
    result = agentledger.run(
        task="smoke",
        agent="custom",
        command=[sys.executable, "-c", "print('ok')"],
        repo=tmp_path,
    )

    assert result.status == "passed"
    assert result.data["stdout_preview"].strip() == "ok"

    report = agentledger.compare(task="smoke", root=tmp_path)
    assert report.run_count == 1
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
