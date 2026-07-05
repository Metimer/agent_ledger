# AgentLedger

AgentLedger is a local-first benchmark ledger for coding agents and OpenAI-compatible LLM providers.

The package is distributed as a Python library with a Rust native core:

```bash
pip install agent-benchmark-ledger
agentledger --help
python -m agentledger --help
```

Current MVP capabilities:

- initialize a local `.agentledger/` store;
- capture a command run with stdout, stderr, duration, exit code and git metadata;
- attach simple eval commands to a run;
- compare runs by task;
- export the ledger as JSONL or CSV;
- open a token-protected local dashboard on `127.0.0.1`;
- use a Python API around the same native core.

Planned next layers are OpenAI-compatible proxy capture, exact LLM token/cost metrics, matrix benchmarks, replay, Parquet/DuckDB analytics and OTLP export.

## Quickstart

```bash
agentledger init
agentledger run --task smoke --agent custom --allow-dirty -- echo ok
agentledger compare smoke
agentledger export --format csv
agentledger dashboard
```

Python:

```python
import agentledger as al

al.init(".")
run = al.run(
    task="smoke",
    agent="custom",
    command=["python", "-c", "print('ok')"],
    allow_dirty=True,
)
report = al.compare(task="smoke")
print(report.to_markdown())
```

## Development

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -U pip maturin pytest
maturin develop
pytest
cargo test
```

## Security Posture

AgentLedger is local-first. It does not send telemetry, bind publicly, or persist API keys by default. The dashboard binds to loopback and requires a per-process token.
