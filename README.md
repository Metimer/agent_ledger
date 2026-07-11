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
- use a Python API around the same native core;
- run a loopback OpenAI-compatible proxy that records LLM call metrics in `.agentledger/llm_calls.ndjson`;
- launch the proxy automatically inside `agentledger run` and attach calls to the captured run;
- stream Server-Sent Events responses through the proxy while recording TTFT and output tokens/s.

Planned next layers are matrix benchmarks, proxy replay, Parquet/DuckDB analytics and OTLP export.

## Quickstart

```bash
agentledger init
agentledger run --task smoke --agent custom --allow-dirty -- echo ok
agentledger run --task provider-smoke --proxy-upstream http://127.0.0.1:11434/v1 -- python my_agent.py
agentledger compare smoke
agentledger export --format csv
agentledger proxy --upstream http://127.0.0.1:11434/v1
agentledger dashboard
```

When `agentledger run` launches a command, it injects `AGENTLEDGER_RUN_ID`, `AGENTLEDGER_ROOT`, and `AGENTLEDGER_PROXY_RUN_HEADER`. With `--proxy-upstream`, it also starts a loopback proxy, injects `OPENAI_BASE_URL`, `OPENAI_API_BASE`, and `AGENTLEDGER_PROXY_URL`, then links every proxied call to the run automatically. Clients that send the `x-agentledger-run-id` header through a separately launched proxy are still aggregated into `agentledger compare`.

Streaming (`stream: true`) responses are relayed chunk-by-chunk; the proxy records time-to-first-token (`ttft_ms`) and output tokens/s per call. Token counts come from the final `usage` chunk when the provider sends one (`source_precision: "exact"`), otherwise they are estimated from the number of content deltas (`source_precision: "estimated"`).

Python:

```python
import agentledger as al

al.init(".")
run = al.run(
    task="smoke",
    agent="custom",
    command=["python", "-c", "print('ok')"],
    allow_dirty=True,
    proxy_upstream="http://127.0.0.1:11434/v1",
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
