use crate::{EvalRecord, LedgerEvent, RunRecord};
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, OpenFlags, Statement};
use serde::Serialize;
use serde_json::{json, Value};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const DB_SCHEMA_VERSION: i32 = 1;

const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  task TEXT NOT NULL,
  agent TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT NOT NULL,
  duration_ms INTEGER NOT NULL,
  exit_code INTEGER,
  repo TEXT NOT NULL,
  source_precision TEXT NOT NULL,
  git_is_repo INTEGER NOT NULL,
  git_base_commit TEXT,
  git_dirty_before INTEGER NOT NULL,
  git_dirty_after INTEGER NOT NULL,
  llm_error_calls INTEGER NOT NULL DEFAULT 0,
  eval_count INTEGER NOT NULL DEFAULT 0,
  eval_status TEXT NOT NULL,
  stdout_path TEXT NOT NULL,
  stderr_path TEXT NOT NULL,
  stdout_preview TEXT NOT NULL,
  stderr_preview TEXT NOT NULL,
  raw TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_task ON runs(task);
CREATE INDEX IF NOT EXISTS idx_runs_agent ON runs(agent);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);

CREATE TABLE IF NOT EXISTS evals (
  run_id TEXT NOT NULL,
  idx INTEGER NOT NULL,
  command TEXT NOT NULL,
  status TEXT NOT NULL,
  exit_code INTEGER,
  duration_ms INTEGER NOT NULL,
  stdout_preview TEXT NOT NULL,
  stderr_preview TEXT NOT NULL,
  PRIMARY KEY (run_id, idx)
);

CREATE TABLE IF NOT EXISTS llm_calls (
  id TEXT PRIMARY KEY,
  run_id TEXT,
  timestamp TEXT,
  endpoint TEXT,
  model TEXT,
  status INTEGER,
  duration_ms INTEGER,
  source_precision TEXT,
  request_stream INTEGER,
  input_tokens INTEGER,
  output_tokens INTEGER,
  total_tokens INTEGER,
  cached_tokens INTEGER,
  reasoning_tokens INTEGER,
  cost_usd REAL,
  ttft_ms INTEGER,
  output_tokens_per_second REAL,
  upstream_base TEXT,
  request_body TEXT,
  response_body TEXT
);
CREATE INDEX IF NOT EXISTS idx_llm_calls_run ON llm_calls(run_id);

CREATE TABLE IF NOT EXISTS sync_state (
  file TEXT PRIMARY KEY,
  byte_offset INTEGER NOT NULL
);
";

#[derive(Debug, Default, Serialize)]
pub struct SyncReport {
    pub runs_upserted: u64,
    pub llm_calls_upserted: u64,
    pub db_path: String,
}

pub fn db_path(root: &Path) -> PathBuf {
    root.join(".agentledger").join("ledger.db")
}

fn open_rw(root: &Path) -> Result<Connection, String> {
    let ledger_dir = root.join(".agentledger");
    fs::create_dir_all(&ledger_dir).map_err(|err| err.to_string())?;
    let conn = Connection::open(db_path(root)).map_err(|err| err.to_string())?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|err| err.to_string())?;
    init_schema(&conn)?;
    Ok(conn)
}

fn init_schema(conn: &Connection) -> Result<(), String> {
    let version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|err| err.to_string())?;
    if version < DB_SCHEMA_VERSION {
        conn.execute_batch(SCHEMA_SQL)
            .map_err(|err| err.to_string())?;
        conn.pragma_update(None, "user_version", DB_SCHEMA_VERSION)
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

/// Rebuild-or-catch-up the SQLite index from the append-only NDJSON files.
/// The NDJSON ledger stays the source of truth; this is safe to call at any
/// time and is a near no-op when nothing new was appended.
pub fn sync(root: &Path) -> Result<SyncReport, String> {
    let conn = open_rw(root)?;
    conn.execute_batch("BEGIN IMMEDIATE")
        .map_err(|err| err.to_string())?;
    let result = sync_inner(&conn, root);
    match result {
        Ok(report) => {
            conn.execute_batch("COMMIT")
                .map_err(|err| err.to_string())?;
            Ok(report)
        }
        Err(err) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(err)
        }
    }
}

fn sync_inner(conn: &Connection, root: &Path) -> Result<SyncReport, String> {
    let runs = sync_events(conn, root)?;
    let calls = sync_llm_calls(conn, root)?;
    Ok(SyncReport {
        runs_upserted: runs,
        llm_calls_upserted: calls,
        db_path: db_path(root).display().to_string(),
    })
}

fn get_offset(conn: &Connection, file: &str) -> Result<u64, String> {
    conn.query_row(
        "SELECT byte_offset FROM sync_state WHERE file = ?1",
        params![file],
        |row| row.get::<_, i64>(0),
    )
    .map(|value| value as u64)
    .or_else(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => Ok(0),
        other => Err(other.to_string()),
    })
}

fn set_offset(conn: &Connection, file: &str, offset: u64) -> Result<(), String> {
    conn.execute(
        "INSERT OR REPLACE INTO sync_state (file, byte_offset) VALUES (?1, ?2)",
        params![file, offset as i64],
    )
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// Iterate the complete lines of `name` starting at the stored offset. A
/// rewritten (smaller) file triggers a full resync; a trailing partial line
/// is left for the next pass.
fn for_each_new_line(
    conn: &Connection,
    root: &Path,
    name: &str,
    mut handle: impl FnMut(&Connection, &str, u64) -> Result<(), String>,
) -> Result<u64, String> {
    let path = root.join(".agentledger").join(name);
    if !path.exists() {
        return Ok(0);
    }
    let size = fs::metadata(&path).map_err(|err| err.to_string())?.len();
    let mut offset = get_offset(conn, name)?;
    if size < offset {
        offset = 0;
    }
    if size == offset {
        return Ok(0);
    }

    let mut file = File::open(&path).map_err(|err| err.to_string())?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|err| err.to_string())?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut processed = 0u64;
    loop {
        line.clear();
        let read = reader.read_line(&mut line).map_err(|err| err.to_string())?;
        if read == 0 {
            break;
        }
        if !line.ends_with('\n') {
            break;
        }
        if !line.trim().is_empty() {
            handle(conn, line.trim(), offset)?;
            processed += 1;
        }
        offset += read as u64;
    }
    set_offset(conn, name, offset)?;
    Ok(processed)
}

fn sync_events(conn: &Connection, root: &Path) -> Result<u64, String> {
    for_each_new_line(conn, root, "events.ndjson", |conn, line, _offset| {
        let event: LedgerEvent = serde_json::from_str(line).map_err(|err| err.to_string())?;
        upsert_run(conn, &event.run)
    })
}

fn sync_llm_calls(conn: &Connection, root: &Path) -> Result<u64, String> {
    for_each_new_line(conn, root, "llm_calls.ndjson", |conn, line, offset| {
        let call: Value = serde_json::from_str(line).map_err(|err| err.to_string())?;
        upsert_llm_call(conn, &call, &format!("offset-{offset}"))
    })
}

fn eval_status(evals: &[EvalRecord]) -> &'static str {
    if evals.is_empty() {
        "not_run"
    } else if evals.iter().all(|eval| eval.status == "passed") {
        "passed"
    } else {
        "failed"
    }
}

fn upsert_run(conn: &Connection, run: &RunRecord) -> Result<(), String> {
    let raw = serde_json::to_string(run).map_err(|err| err.to_string())?;
    conn.execute(
        "INSERT OR REPLACE INTO runs (
            id, task, agent, status, started_at, ended_at, duration_ms, exit_code,
            repo, source_precision, git_is_repo, git_base_commit, git_dirty_before,
            git_dirty_after, llm_error_calls, eval_count, eval_status,
            stdout_path, stderr_path, stdout_preview, stderr_preview, raw
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
        params![
            run.id,
            run.task,
            run.agent,
            run.status,
            run.started_at,
            run.ended_at,
            run.duration_ms as i64,
            run.exit_code,
            run.repo,
            run.source_precision,
            run.git.is_git_repo,
            run.git.base_commit,
            run.git.dirty_before,
            run.git.dirty_after,
            run.llm_error_calls as i64,
            run.evals.len() as i64,
            eval_status(&run.evals),
            run.stdout_path,
            run.stderr_path,
            run.stdout_preview,
            run.stderr_preview,
            raw,
        ],
    )
    .map_err(|err| err.to_string())?;

    conn.execute("DELETE FROM evals WHERE run_id = ?1", params![run.id])
        .map_err(|err| err.to_string())?;
    for (idx, eval) in run.evals.iter().enumerate() {
        conn.execute(
            "INSERT INTO evals (run_id, idx, command, status, exit_code, duration_ms, stdout_preview, stderr_preview)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                run.id,
                idx as i64,
                eval.command,
                eval.status,
                eval.exit_code,
                eval.duration_ms as i64,
                eval.stdout_preview,
                eval.stderr_preview,
            ],
        )
        .map_err(|err| err.to_string())?;
    }
    Ok(())
}

fn upsert_llm_call(conn: &Connection, call: &Value, fallback_id: &str) -> Result<(), String> {
    let metrics = call.get("metrics");
    let field_u64 = |name: &str| {
        metrics
            .and_then(|metrics| metrics.get(name))
            .and_then(Value::as_u64)
            .map(|value| value as i64)
    };
    let body_text = |name: &str| {
        call.get(name)
            .filter(|value| !value.is_null())
            .map(|value| value.to_string())
    };
    conn.execute(
        "INSERT OR REPLACE INTO llm_calls (
            id, run_id, timestamp, endpoint, model, status, duration_ms,
            source_precision, request_stream, input_tokens, output_tokens,
            total_tokens, cached_tokens, reasoning_tokens, cost_usd, ttft_ms,
            output_tokens_per_second, upstream_base, request_body, response_body
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)",
        params![
            call.get("id")
                .and_then(Value::as_str)
                .unwrap_or(fallback_id),
            call.get("run_id").and_then(Value::as_str),
            call.get("timestamp").and_then(Value::as_str),
            call.get("endpoint").and_then(Value::as_str),
            call.get("model").and_then(Value::as_str),
            call.get("status").and_then(Value::as_u64).map(|v| v as i64),
            call.get("duration_ms")
                .and_then(Value::as_u64)
                .map(|v| v as i64),
            call.get("source_precision").and_then(Value::as_str),
            call.get("request_stream").and_then(Value::as_bool),
            field_u64("input_tokens"),
            field_u64("output_tokens"),
            field_u64("total_tokens"),
            field_u64("cached_tokens"),
            field_u64("reasoning_tokens"),
            metrics
                .and_then(|metrics| metrics.get("cost_usd"))
                .and_then(Value::as_f64),
            field_u64("ttft_ms"),
            metrics
                .and_then(|metrics| metrics.get("output_tokens_per_second"))
                .and_then(Value::as_f64),
            call.get("upstream_base").and_then(Value::as_str),
            body_text("request_body"),
            body_text("response_body"),
        ],
    )
    .map_err(|err| err.to_string())?;
    Ok(())
}

/// Run an arbitrary read-only SQL query against the synced index.
pub fn query(root: &Path, sql: &str) -> Result<Vec<Value>, String> {
    sync(root)?;
    let conn = Connection::open_with_flags(
        db_path(root),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|err| err.to_string())?;
    let mut stmt = conn.prepare(sql).map_err(|err| err.to_string())?;
    rows_to_json(&mut stmt, [])
}

fn rows_to_json<P: rusqlite::Params>(
    stmt: &mut Statement<'_>,
    params: P,
) -> Result<Vec<Value>, String> {
    let names = stmt
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let mut rows = stmt.query(params).map_err(|err| err.to_string())?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(|err| err.to_string())? {
        let mut object = serde_json::Map::new();
        for (index, name) in names.iter().enumerate() {
            let value = match row.get_ref(index).map_err(|err| err.to_string())? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(value) => json!(value),
                ValueRef::Real(value) => json!(value),
                ValueRef::Text(text) => json!(String::from_utf8_lossy(text)),
                ValueRef::Blob(blob) => json!(format!("<{} bytes blob>", blob.len())),
            };
            object.insert(name.clone(), value);
        }
        out.push(Value::Object(object));
    }
    Ok(out)
}

pub struct RunFilters<'a> {
    pub task: Option<&'a str>,
    pub agent: Option<&'a str>,
    pub status: Option<&'a str>,
    pub since: Option<&'a str>,
    pub limit: i64,
}

pub fn list_runs(root: &Path, filters: &RunFilters<'_>) -> Result<Vec<Value>, String> {
    let conn = open_rw(root)?;
    let mut stmt = conn
        .prepare(
            "SELECT r.id, r.task, r.agent, r.status, r.started_at, r.duration_ms, r.exit_code,
                    r.eval_status, r.eval_count, r.llm_error_calls, r.repo,
                    COUNT(c.id) AS llm_call_count,
                    SUM(c.input_tokens) AS token_input,
                    SUM(c.output_tokens) AS token_output,
                    SUM(c.total_tokens) AS token_total,
                    SUM(c.cost_usd) AS cost_usd,
                    MIN(c.ttft_ms) AS ttft_ms
             FROM runs r
             LEFT JOIN llm_calls c ON c.run_id = r.id
             WHERE (?1 IS NULL OR r.task = ?1)
               AND (?2 IS NULL OR r.agent = ?2)
               AND (?3 IS NULL OR r.status = ?3)
               AND (?4 IS NULL OR r.started_at >= ?4)
             GROUP BY r.id
             ORDER BY r.started_at DESC
             LIMIT ?5",
        )
        .map_err(|err| err.to_string())?;
    rows_to_json(
        &mut stmt,
        params![
            filters.task,
            filters.agent,
            filters.status,
            filters.since,
            filters.limit
        ],
    )
}

pub fn run_detail(root: &Path, run_id: &str) -> Result<Option<Value>, String> {
    let conn = open_rw(root)?;
    let raw: Option<String> = conn
        .query_row(
            "SELECT raw FROM runs WHERE id = ?1",
            params![run_id],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|err| match err {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other.to_string()),
        })?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let run: Value = serde_json::from_str(&raw).map_err(|err| err.to_string())?;

    let mut calls_stmt = conn
        .prepare(
            "SELECT id, timestamp, endpoint, model, status, duration_ms, source_precision,
                    request_stream, input_tokens, output_tokens, total_tokens, cached_tokens,
                    reasoning_tokens, cost_usd, ttft_ms, output_tokens_per_second,
                    upstream_base, request_body, response_body
             FROM llm_calls WHERE run_id = ?1 ORDER BY timestamp",
        )
        .map_err(|err| err.to_string())?;
    let llm_calls = rows_to_json(&mut calls_stmt, params![run_id])?;

    Ok(Some(json!({ "run": run, "llm_calls": llm_calls })))
}

pub fn run_output_path(root: &Path, run_id: &str, stream: &str) -> Result<Option<String>, String> {
    let column = match stream {
        "stderr" => "stderr_path",
        _ => "stdout_path",
    };
    let conn = open_rw(root)?;
    conn.query_row(
        &format!("SELECT {column} FROM runs WHERE id = ?1"),
        params![run_id],
        |row| row.get::<_, String>(0),
    )
    .map(Some)
    .or_else(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => Ok(None),
        other => Err(other.to_string()),
    })
}

pub fn task_aggregates(root: &Path) -> Result<Vec<Value>, String> {
    let conn = open_rw(root)?;
    let mut stmt = conn
        .prepare(
            "SELECT r.task, r.agent,
                    COUNT(*) AS runs,
                    SUM(CASE WHEN r.status = 'passed' THEN 1 ELSE 0 END) AS passed,
                    AVG(r.duration_ms) AS avg_duration_ms,
                    MIN(r.duration_ms) AS min_duration_ms,
                    AVG(c.ttft) AS avg_ttft_ms,
                    SUM(c.tokens_in) AS token_input,
                    SUM(c.tokens_out) AS token_output,
                    SUM(c.cost) AS cost_usd,
                    AVG(c.tps) AS avg_output_tokens_per_second,
                    MAX(r.started_at) AS last_run_at
             FROM runs r
             LEFT JOIN (
                 SELECT run_id,
                        MIN(ttft_ms) AS ttft,
                        SUM(input_tokens) AS tokens_in,
                        SUM(output_tokens) AS tokens_out,
                        SUM(cost_usd) AS cost,
                        AVG(output_tokens_per_second) AS tps
                 FROM llm_calls GROUP BY run_id
             ) c ON c.run_id = r.id
             GROUP BY r.task, r.agent
             ORDER BY r.task, r.agent",
        )
        .map_err(|err| err.to_string())?;
    rows_to_json(&mut stmt, [])
}

pub fn timeseries(root: &Path, task: Option<&str>) -> Result<Vec<Value>, String> {
    let conn = open_rw(root)?;
    let mut stmt = conn
        .prepare(
            "SELECT substr(r.started_at, 1, 10) AS day,
                    COUNT(*) AS runs,
                    SUM(CASE WHEN r.status = 'passed' THEN 1 ELSE 0 END) AS passed,
                    AVG(r.duration_ms) AS avg_duration_ms,
                    SUM(COALESCE(c.cost, 0)) AS cost_usd,
                    SUM(COALESCE(c.tokens, 0)) AS token_total
             FROM runs r
             LEFT JOIN (
                 SELECT run_id, SUM(cost_usd) AS cost, SUM(total_tokens) AS tokens
                 FROM llm_calls GROUP BY run_id
             ) c ON c.run_id = r.id
             WHERE (?1 IS NULL OR r.task = ?1)
             GROUP BY day
             ORDER BY day",
        )
        .map_err(|err| err.to_string())?;
    rows_to_json(&mut stmt, params![task])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{append_run_event, GitSnapshot, RunMetrics};
    use uuid::Uuid;

    fn temp_root() -> PathBuf {
        let root = std::env::temp_dir().join(format!("agentledger-db-test-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join(".agentledger")).expect("create test ledger");
        root
    }

    fn sample_run(id: &str, root: &Path) -> RunRecord {
        RunRecord {
            id: id.to_string(),
            task: "t".to_string(),
            agent: "a".to_string(),
            command: vec!["true".to_string()],
            repo: root.to_string_lossy().to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            ended_at: "2026-01-01T00:00:01Z".to_string(),
            duration_ms: 1000,
            exit_code: Some(0),
            status: "passed".to_string(),
            source_precision: "observed".to_string(),
            stdout_path: String::new(),
            stderr_path: String::new(),
            stdout_preview: String::new(),
            stderr_preview: String::new(),
            git: GitSnapshot {
                is_git_repo: false,
                base_commit: None,
                dirty_before: false,
                dirty_after: false,
                diffstat: None,
            },
            evals: vec![],
            metrics: RunMetrics::default(),
            llm_error_calls: 0,
        }
    }

    fn write_llm_call(root: &Path, run_id: &str, total_tokens: u64, ttft_ms: u64) {
        let line = json!({
            "id": Uuid::new_v4().to_string(),
            "run_id": run_id,
            "model": "mock",
            "status": 200,
            "duration_ms": 1000,
            "metrics": {
                "input_tokens": total_tokens / 2,
                "output_tokens": total_tokens / 2,
                "total_tokens": total_tokens,
                "cost_usd": 0.001,
                "ttft_ms": ttft_ms,
            }
        });
        let path = root.join(".agentledger").join("llm_calls.ndjson");
        let mut existing = fs::read_to_string(&path).unwrap_or_default();
        existing.push_str(&line.to_string());
        existing.push('\n');
        fs::write(path, existing).expect("write llm call");
    }

    #[test]
    fn sync_is_incremental_and_idempotent() {
        let root = temp_root();
        let ledger_dir = root.join(".agentledger");
        append_run_event(&ledger_dir, &sample_run("run-1", &root)).expect("append run");
        write_llm_call(&root, "run-1", 10, 120);

        let first = sync(&root).expect("first sync");
        assert_eq!(first.runs_upserted, 1);
        assert_eq!(first.llm_calls_upserted, 1);

        // Nothing new appended: pure no-op.
        let second = sync(&root).expect("second sync");
        assert_eq!(second.runs_upserted, 0);
        assert_eq!(second.llm_calls_upserted, 0);

        // Incremental catch-up from the stored offset.
        append_run_event(&ledger_dir, &sample_run("run-2", &root)).expect("append run 2");
        write_llm_call(&root, "run-2", 20, 80);
        let third = sync(&root).expect("third sync");
        assert_eq!(third.runs_upserted, 1);
        assert_eq!(third.llm_calls_upserted, 1);

        let rows = query(&root, "SELECT count(*) AS n FROM runs").expect("count runs");
        assert_eq!(rows[0]["n"], json!(2));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn latest_run_version_wins_after_reappend() {
        let root = temp_root();
        let ledger_dir = root.join(".agentledger");
        append_run_event(&ledger_dir, &sample_run("run-1", &root)).expect("append run");

        // A post-hoc eval appends an updated version of the same run.
        let mut updated = sample_run("run-1", &root);
        updated.status = "failed".to_string();
        updated.evals.push(crate::EvalRecord {
            command: "exit 1".to_string(),
            exit_code: Some(1),
            duration_ms: 5,
            status: "failed".to_string(),
            stdout_preview: String::new(),
            stderr_preview: String::new(),
        });
        append_run_event(&ledger_dir, &updated).expect("append updated run");

        sync(&root).expect("sync");
        let rows = query(
            &root,
            "SELECT status, eval_count, eval_status FROM runs WHERE id = 'run-1'",
        )
        .expect("query run");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["status"], json!("failed"));
        assert_eq!(rows[0]["eval_count"], json!(1));
        assert_eq!(rows[0]["eval_status"], json!("failed"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn query_is_read_only() {
        let root = temp_root();
        append_run_event(&root.join(".agentledger"), &sample_run("run-1", &root))
            .expect("append run");
        sync(&root).expect("sync");

        let err = query(&root, "INSERT INTO runs (id) VALUES ('hack')")
            .expect_err("write must be rejected");
        assert!(
            err.contains("readonly") || err.contains("read-only"),
            "{err}"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn aggregates_join_llm_metrics() {
        let root = temp_root();
        let ledger_dir = root.join(".agentledger");
        let mut run = sample_run("run-1", &root);
        run.task = "bench".to_string();
        run.agent = "model-a".to_string();
        append_run_event(&ledger_dir, &run).expect("append run");
        write_llm_call(&root, "run-1", 100, 500);
        sync(&root).expect("sync");

        let aggregates = task_aggregates(&root).expect("aggregates");
        let row = aggregates
            .iter()
            .find(|row| row["task"] == json!("bench"))
            .expect("bench row");
        assert_eq!(row["runs"], json!(1));
        assert_eq!(row["passed"], json!(1));
        assert_eq!(row["token_output"], json!(50));
        assert_eq!(row["avg_ttft_ms"], json!(500.0));

        let series = timeseries(&root, Some("bench")).expect("timeseries");
        assert_eq!(series.len(), 1);
        assert_eq!(series[0]["day"], json!("2026-01-01"));
        assert_eq!(series[0]["token_total"], json!(100));

        let runs = list_runs(
            &root,
            &RunFilters {
                task: Some("bench"),
                agent: None,
                status: None,
                since: None,
                limit: 10,
            },
        )
        .expect("list runs");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["llm_call_count"], json!(1));
        assert_eq!(runs[0]["token_total"], json!(100));

        let _ = fs::remove_dir_all(&root);
    }
}
