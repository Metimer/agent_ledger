# Test manuel AgentLedger — protocole pas à pas

Check-list de validation manuelle avant release. Chaque étape indique la commande à lancer et le résultat attendu. Durée totale : ~15 minutes.

## 0. Prérequis

- Python ≥ 3.10, Rust stable, git.
- Optionnel (étape 7b) : un serveur OpenAI-compatible réel, p.ex. Ollama avec un petit modèle (`ollama pull qwen2.5:0.5b`).

## 1. Build et installation

```bash
cd ~/DEV/agent_ledger
python -m venv .venv && . .venv/bin/activate
pip install -U pip maturin pytest
maturin develop
```

**Attendu** : se termine par `🛠 Installed agent-benchmark-ledger-0.1.0`, sans erreur de compilation.

```bash
agentledger --help
```

**Attendu** : l'aide liste les commandes `init`, `run`, `bench`, `compare`, `replay`, `eval`, `dashboard`, `proxy`, `export`, `providers`, `agents`, `doctor`.

## 2. Suites de tests automatiques

```bash
cargo test && pytest -q
```

**Attendu** : `20 passed` côté Rust, `7 passed` côté Python, aucun échec.

## 3. Projet de test et init

```bash
mkdir /tmp/al-demo && cd /tmp/al-demo
git init && git commit --allow-empty -m init
agentledger init
```

**Attendu** :
- Message `Initialized AgentLedger ...`.
- `AgentLedger.toml` créé (config avec agents `codex`/`claude-code`/`opencode`/`custom` et providers `ollama`/`openrouter`/...).
- Dossier `.agentledger/` créé avec permissions `700` (`ls -ld .agentledger` → `drwx------`).

## 4. Run simple

```bash
agentledger run --task smoke --agent custom -- sh -c 'echo bonjour; echo $AGENTLEDGER_RUN_ID'
```

**Attendu** :
- JSON du run sur stdout : `"task": "smoke"`, `"status": "passed"`, `"exit_code": 0`, `"duration_ms"` renseigné.
- `stdout_preview` contient `bonjour` **et** un UUID identique au champ `"id"` (preuve que `AGENTLEDGER_RUN_ID` est injecté).
- `git.base_commit` renseigné, `dirty_before: false`.
- `.agentledger/runs/<id>/stdout.txt` et `stderr.txt` existent.
- `.agentledger/events.ndjson` contient 1 ligne avec `"hash"` et `"previous_hash": null`.

## 5. Refus de repo sale (garde-fou)

```bash
echo x > pollution.txt
agentledger run --task smoke -- echo test
```

**Attendu** : erreur `capture error: ... dirty ...` (le run est refusé). Puis :

```bash
agentledger run --task smoke --allow-dirty -- echo test
rm pollution.txt
```

**Attendu** : le run passe avec `--allow-dirty`.

## 6. Run avec proxy intégré (mock upstream)

Créer le mock OpenAI-compatible :

```bash
cat > /tmp/mock_openai.py <<'EOF'
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["content-length"])))
        if body.get("stream"):
            self.send_response(200)
            self.send_header("content-type", "text/event-stream")
            self.end_headers()
            for part in ["Bon", "jour"]:
                chunk = {"model": "mock", "choices": [{"delta": {"content": part}}]}
                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode()); self.wfile.flush()
            usage = {"choices": [], "usage": {"prompt_tokens": 11, "completion_tokens": 5, "total_tokens": 16}}
            self.wfile.write(f"data: {json.dumps(usage)}\n\ndata: [DONE]\n\n".encode())
        else:
            payload = json.dumps({"model": "mock", "choices": [{"message": {"role": "assistant", "content": "Bonjour"}}],
                                  "usage": {"prompt_tokens": 11, "completion_tokens": 5, "total_tokens": 16}}).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
    def log_message(self, *a): pass

HTTPServer(("127.0.0.1", 4141), H).serve_forever()
EOF
python3 /tmp/mock_openai.py &
```

Puis le run proxifié :

```bash
agentledger run --task llm-smoke --allow-dirty --proxy-upstream http://127.0.0.1:4141/v1 -- \
  sh -c 'curl -sS -X POST "$OPENAI_BASE_URL/chat/completions" -H "content-type: application/json" -d "{\"model\":\"mock\",\"messages\":[]}"'
```

**Attendu** :
- Une ligne `AgentLedger OpenAI-compatible proxy: http://127.0.0.1:<port>/v1` (port éphémère).
- `stdout_preview` du run contient la réponse JSON du mock (`"Bonjour"`).
- `.agentledger/llm_calls.ndjson` contient 1 enregistrement : `"run_id"` = id du run, `"model": "mock"`, `"status": 200`, `"source_precision": "exact"`, `metrics.input_tokens: 11`, `output_tokens: 5`, `total_tokens: 16`, `cost_usd: null`, `ttft_ms: null` (pas de streaming ici).

## 7. Streaming SSE à travers le proxy

### 7a. Avec le mock (toujours lancé)

```bash
agentledger run --task llm-stream --allow-dirty --proxy-upstream http://127.0.0.1:4141/v1 -- \
  sh -c 'curl -sS -N -X POST "$OPENAI_BASE_URL/chat/completions" -H "content-type: application/json" -d "{\"model\":\"mock\",\"stream\":true,\"messages\":[]}"'
```

**Attendu** :
- `stdout_preview` contient les événements SSE bruts : `data: {"model":"mock",...`Bon`...`, `data: [DONE]` (relayés chunk par chunk, pas bufferisés).
- Dernière ligne de `.agentledger/llm_calls.ndjson` : `"request_stream": true`, `"source_precision": "exact"`, `metrics.total_tokens: 16`, **`ttft_ms` renseigné** (entier ≥ 0), `output_tokens_per_second` renseigné.

Arrêter le mock : `kill %1`.

### 7b. (Optionnel) Avec Ollama réel

```bash
agentledger proxy --upstream http://127.0.0.1:11434/v1 &
# noter le port affiché, puis :
curl -sS -N -X POST http://127.0.0.1:<port>/v1/chat/completions \
  -H "content-type: application/json" \
  -d '{"model":"qwen2.5:0.5b","stream":true,"messages":[{"role":"user","content":"Dis bonjour"}]}'
kill %1
```

**Attendu** : les tokens s'affichent **au fil de l'eau** (pas d'un bloc à la fin) ; le dernier enregistrement de `llm_calls.ndjson` a `ttft_ms` > 0 et des compteurs de tokens (exacts si Ollama envoie `usage`, sinon `source_precision: "estimated"`).

## 8. Bench matrix

```bash
cat > bench.toml <<'EOF'
repeats = 2
allow_dirty = true

[[tasks]]
name = "greet"
prompt = "bonjour"
evals = ["test -f bench.toml"]

[[tasks]]
name = "date"

[[agents]]
name = "echo"
command = ["sh", "-c", "echo {prompt} depuis {task}"]
EOF
agentledger bench --matrix bench.toml
```

**Attendu** :
- Progression sur stderr : `bench: task=greet agent=echo repeat=1/2` etc.
- JSON final : `"cell_count": 4`, `"passed": 4`, `"failed": 0`, 4 cellules avec `run_id`, `"repeat": 1|2`, `"provider": null`.
- `agentledger bench --matrix bench.toml --task greet` → `"cell_count": 2` (filtre).
- Une matrice sans `[[agents]]` → `configuration error: bench matrix needs at least one [[agents]] entry`.

## 9. Eval post-hoc

```bash
RUN_ID=$(agentledger compare greet | python3 -c "import json,sys; print(json.load(sys.stdin)['runs'][0]['id'])")
agentledger eval "$RUN_ID" --test "test -f bench.toml" --test "test -f README.md"
```

**Attendu** :
- JSON du run mis à jour : le tableau `evals` contient maintenant l'éval d'origine + les 2 nouvelles ; la 2ᵉ nouvelle (`README.md` absent dans /tmp/al-demo) est `"status": "failed"` → statut global du run `"failed"`.
- `agentledger compare greet` → **toujours le même nombre de runs** (pas de doublon), avec `eval_status: "failed"` pour ce run.
- `agentledger eval inconnu --test "true"` → `storage error: run 'inconnu' not found in ledger`.

## 10. Compare et export

```bash
agentledger compare            # tous les runs
agentledger compare llm-stream
agentledger export --format csv --output runs.csv
agentledger export --format jsonl
```

**Attendu** :
- `compare` global : `run_count` = total des runs (4 + 4 bench + ...), chaque run avec `duration_ms`, `eval_status`, `llm_metrics_precision`.
- `compare llm-stream` : 1 run, `token_total: 16`, `llm_call_count: 1`, `llm_metrics_precision: "exact"`.
- `runs.csv` : en-tête `id,task,agent,status,...` + une ligne par run.
- `export --format parquet` → erreur explicite « planned for the analytics extra ».

## 11. Dashboard

```bash
agentledger dashboard &
```

**Attendu** :
- Ligne `AgentLedger dashboard: http://127.0.0.1:<port>/?token=<uuid>`.
- Ouvrir l'URL **avec** token : tableau HTML des runs (ID, Task, Agent, Status, Duration, Eval, LLM Precision).
- `curl http://127.0.0.1:<port>/` **sans** token → `401 missing token`.
- `curl "http://127.0.0.1:<port>/api/runs?token=<uuid>"` → JSON du compare.
- `kill %1` pour arrêter.

## 12. API Python

```bash
python3 - <<'EOF'
import agentledger as al
report = al.compare(root="/tmp/al-demo")
print(report.run_count, "runs")
print(report.to_markdown())
EOF
```

**Attendu** : le nombre de runs cohérent avec l'étape 10 et un tableau Markdown propre (`| Run | Task | Agent | ... |`).

## 13. Doctor

```bash
agentledger doctor
```

**Attendu** : version, `ledger: present`, statut de `git`, liste des agents (`codex/claude-code/opencode` trouvés ou `not found` selon la machine) et des providers configurés.

## Nettoyage

```bash
kill %1 2>/dev/null; rm -rf /tmp/al-demo /tmp/mock_openai.py
```
