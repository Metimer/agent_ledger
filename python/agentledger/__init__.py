"""Python API for AgentLedger."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from . import _native

__version__ = _native.version()

AgentLedgerError = _native.AgentLedgerError
ConfigError = _native.ConfigError
CaptureError = _native.CaptureError
StorageError = _native.StorageError
ReplayError = _native.ReplayError
ProviderError = _native.ProviderError
SecurityError = _native.SecurityError


@dataclass(frozen=True)
class RunResult:
    data: dict[str, Any]

    @property
    def id(self) -> str:
        return str(self.data["id"])

    @property
    def status(self) -> str:
        return str(self.data["status"])

    def to_dict(self) -> dict[str, Any]:
        return dict(self.data)

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.data, indent=indent)


@dataclass(frozen=True)
class BenchReport:
    data: dict[str, Any]

    @property
    def cell_count(self) -> int:
        return int(self.data["cell_count"])

    @property
    def passed(self) -> int:
        return int(self.data["passed"])

    @property
    def failed(self) -> int:
        return int(self.data["failed"])

    def to_dict(self) -> dict[str, Any]:
        return dict(self.data)

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.data, indent=indent)


@dataclass(frozen=True)
class CompareReport:
    data: dict[str, Any]

    @property
    def run_count(self) -> int:
        return int(self.data["run_count"])

    def to_dict(self) -> dict[str, Any]:
        return dict(self.data)

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.data, indent=indent)

    def to_markdown(self) -> str:
        rows = self.data.get("runs", [])
        header = "| Run | Task | Agent | Status | Duration | Eval | Tokens | LLM calls | LLM metrics |\n"
        separator = "| --- | --- | --- | --- | ---: | --- | ---: | ---: | --- |\n"
        body = "".join(
            "| {id} | {task} | {agent} | {status} | {duration_ms} ms | {eval_status} | {token_total} | {llm_call_count} | {llm_metrics_precision} |\n".format(
                id=str(row["id"])[:8],
                task=row["task"],
                agent=row["agent"],
                status=row["status"],
                duration_ms=row["duration_ms"],
                eval_status=row["eval_status"],
                token_total=row.get("token_total") or 0,
                llm_call_count=row.get("llm_call_count", 0),
                llm_metrics_precision=row["llm_metrics_precision"],
            )
            for row in rows
        )
        return header + separator + body

    def to_dataframe(self):  # type: ignore[no-untyped-def]
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError(
                "Install analytics extras first: pip install agent-benchmark-ledger[analytics]"
            ) from exc
        return pd.DataFrame(self.data.get("runs", []))


class AgentLedger:
    def __init__(self, root: str | Path = ".") -> None:
        self.root = Path(root)

    def init(self) -> str:
        return init(self.root)

    def run(
        self,
        *,
        task: str,
        agent: str = "custom",
        command: Iterable[str],
        eval_commands: Iterable[str] | None = None,
        allow_dirty: bool = False,
        proxy_upstream: str | None = None,
        proxy_bind: str = "127.0.0.1:0",
        proxy_api_key_env: str | None = None,
        proxy_record_bodies: bool = False,
    ) -> RunResult:
        return run(
            task=task,
            agent=agent,
            command=command,
            repo=self.root,
            eval_commands=eval_commands,
            allow_dirty=allow_dirty,
            proxy_upstream=proxy_upstream,
            proxy_bind=proxy_bind,
            proxy_api_key_env=proxy_api_key_env,
            proxy_record_bodies=proxy_record_bodies,
        )

    def bench(self, *, matrix: str | Path, task: str | None = None) -> BenchReport:
        return bench(matrix=matrix, repo=self.root, task=task)

    def compare(self, task: str | None = None) -> CompareReport:
        return compare(task=task, root=self.root)

    def export(
        self,
        *,
        format: str = "jsonl",
        output: str | Path | None = None,
    ) -> str:
        return export(format=format, output=output, root=self.root)

    def doctor(self) -> str:
        return doctor(self.root)


def init(path: str | Path = ".") -> str:
    return _native.init_project(str(path))


def run(
    *,
    task: str,
    agent: str = "custom",
    command: Iterable[str],
    repo: str | Path = ".",
    eval_commands: Iterable[str] | None = None,
    allow_dirty: bool = False,
    proxy_upstream: str | None = None,
    proxy_bind: str = "127.0.0.1:0",
    proxy_api_key_env: str | None = None,
    proxy_record_bodies: bool = False,
) -> RunResult:
    payload = _native.run_task(
        task,
        agent,
        [str(part) for part in command],
        str(repo),
        [str(part) for part in eval_commands] if eval_commands is not None else None,
        allow_dirty,
        proxy_upstream,
        proxy_bind,
        proxy_api_key_env,
        proxy_record_bodies,
    )
    return RunResult(json.loads(payload))


def bench(
    *,
    matrix: str | Path,
    repo: str | Path = ".",
    task: str | None = None,
) -> BenchReport:
    payload = _native.bench_matrix(str(matrix), str(repo), task)
    return BenchReport(json.loads(payload))


def compare(task: str | None = None, root: str | Path = ".") -> CompareReport:
    payload = _native.compare_runs(task, str(root))
    return CompareReport(json.loads(payload))


def replay(*args: Any, **kwargs: Any) -> None:
    raise NotImplementedError("replay will land after proxy capture")


def eval(*args: Any, **kwargs: Any) -> None:  # noqa: A001
    raise NotImplementedError("post-hoc eval will land after run-time eval commands")


def export(
    *,
    format: str = "jsonl",
    output: str | Path | None = None,
    root: str | Path = ".",
) -> str:
    return _native.export_ledger(format, str(output) if output is not None else None, str(root))


def open_dashboard(*, bind: str = "127.0.0.1:0", root: str | Path = ".") -> int:
    return int(_native.run_cli(["dashboard", "--bind", bind, "--root", str(root)]))


def openai_proxy(
    *,
    upstream: str,
    bind: str = "127.0.0.1:0",
    root: str | Path = ".",
    api_key_env: str | None = None,
    record_bodies: bool = False,
) -> None:
    _native.start_proxy(bind, upstream, str(root), api_key_env, record_bodies)


def doctor(root: str | Path = ".") -> str:
    return _native.doctor(str(root))


__all__ = [
    "AgentLedger",
    "AgentLedgerError",
    "BenchReport",
    "CaptureError",
    "CompareReport",
    "ConfigError",
    "ProviderError",
    "ReplayError",
    "RunResult",
    "SecurityError",
    "StorageError",
    "__version__",
    "bench",
    "compare",
    "doctor",
    "eval",
    "export",
    "init",
    "open_dashboard",
    "openai_proxy",
    "replay",
    "run",
]
