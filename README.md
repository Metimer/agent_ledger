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
- index every run and LLM call into an embedded SQLite database for SQL queries;
- open a token-protected local dashboard on `127.0.0.1` with filterable runs, per-task agent comparison charts, per-prompt provider/model comparison charts, run details and daily trends;
- use a Python API around the same native core;
- run a loopback OpenAI-compatible proxy that records LLM call metrics in `.agentledger/llm_calls.ndjson`;
- launch the proxy automatically inside `agentledger run` and attach calls to the captured run;
- stream Server-Sent Events responses through the proxy while recording TTFT and output tokens/s;
- run benchmark matrices (tasks × agents × providers × repeats) from a TOML file;
- re-evaluate an existing run post-hoc with `agentledger eval <run-id> --test <cmd>`.

Planned next layers are proxy replay, Parquet export, OTLP export and a PostgreSQL export target for team-wide aggregation.

## Quickstart

```bash
agentledger init
agentledger run --task smoke --agent custom --allow-dirty -- echo ok
agentledger run --task provider-smoke --proxy-upstream http://127.0.0.1:11434/v1 -- python my_agent.py
agentledger bench --matrix bench.toml
agentledger eval <run-id> --test "pytest -q"
agentledger compare smoke
agentledger export --format csv
agentledger proxy --upstream http://127.0.0.1:11434/v1
agentledger dashboard
```

When `agentledger run` launches a command, it injects `AGENTLEDGER_RUN_ID`, `AGENTLEDGER_ROOT`, and `AGENTLEDGER_PROXY_RUN_HEADER`. With `--proxy-upstream`, it also starts a loopback proxy, injects `OPENAI_BASE_URL`, `OPENAI_API_BASE`, and `AGENTLEDGER_PROXY_URL`, then links every proxied call to the run automatically. Clients that send the `x-agentledger-run-id` header through a separately launched proxy are still aggregated into `agentledger compare`.

Streaming (`stream: true`) responses are relayed chunk-by-chunk; the proxy records time-to-first-token (`ttft_ms`) and output tokens/s per call. Token counts come from the final `usage` chunk when the provider sends one (`source_precision: "exact"`), otherwise they are estimated from the number of content deltas (`source_precision: "estimated"`).

Every run records `llm_error_calls`, the number of proxied calls that failed (HTTP >= 400 or unreachable upstream). Clients like `curl` exit 0 on an HTTP 429, so a run can look `passed` while its LLM calls failed; pass `--fail-on-llm-error` (CLI/Python) or set `fail_on_llm_error = true` in a bench matrix to mark such runs `failed`.

## Benchmark matrices

`agentledger bench --matrix bench.toml [--repo .] [--task name]` runs every task × agent × provider × repeat combination through the same capture pipeline as `agentledger run`, then prints a JSON report with one cell per run. `{prompt}` and `{task}` placeholders in agent commands are substituted per task; each provider starts a loopback proxy so LLM calls are attached to their run. Cells keep executing even when one fails.

```toml
repeats = 2
allow_dirty = true
fail_on_llm_error = true

[[tasks]]
name = "fix-bug"
prompt = "Fix the failing test in this repo"
evals = ["pytest -q"]

[[agents]]
name = "claude-code"
command = ["claude", "-p", "{prompt}"]

[[agents]]
name = "codex"
command = ["codex", "exec", "{prompt}"]

[[providers]]
name = "ollama"
upstream = "http://127.0.0.1:11434/v1"
# api_key_env = "OPENROUTER_API_KEY"
# record_bodies = false
```

Python: `al.bench(matrix="bench.toml", repo=".", task=None)` returns a `BenchReport` with `cell_count`, `passed`, `failed` and per-cell run ids; runs land in the same ledger, so `al.compare(task=...)` aggregates them.

## Analytics: SQLite index and dashboard

The NDJSON ledger stays the source of truth; `agentledger db sync` builds (and incrementally catches up) a rebuildable SQLite index at `.agentledger/ledger.db` with `runs`, `evals` and `llm_calls` tables. Query it with arbitrary read-only SQL:

```bash
agentledger db sync
agentledger db query "SELECT task, agent, count(*) runs, avg(duration_ms) avg_ms FROM runs GROUP BY 1, 2"
```

Python: `al.sync_db(root=".")` and `al.query("SELECT ...", root=".")` (rows as dicts, resynced automatically).

`agentledger dashboard` serves a token-protected local UI on loopback backed by the same index: a filterable/sortable runs table, per-task agent comparison charts (duration, TTFT, tokens, cost, tokens/s), a provider/model comparison view that groups LLM calls by model and upstream — filterable by task and by exact prompt, with the compared prompt displayed in full — a run detail view (evals, LLM calls with the captured prompt, metrics and recorded bodies, stdout/stderr) and daily trend charts. JSON endpoints (`/api/runs`, `/api/runs/{id}`, `/api/runs/{id}/output`, `/api/tasks`, `/api/models`, `/api/prompts`, `/api/timeseries`) are available with the same token for scripting.

The proxy captures the user prompt of each call (truncated to 2000 chars) so the dashboard can compare providers and models at equal prompt. Set `privacy.capture_prompts = false` in `AgentLedger.toml` to opt out; recorded bodies (`record_bodies`) always imply prompt capture.

## Post-hoc evals

`agentledger eval <run-id> --test "pytest -q" [--root .]` (or `al.eval(run_id=..., tests=[...], root=...)`) re-runs eval commands against the repo recorded for an existing run, appends the results to its eval list and recomputes its status. The ledger stays append-only: the updated run is appended as a new hash-chained event with the same id, and readers keep the latest version.

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
