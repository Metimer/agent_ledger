use clap::{Parser, Subcommand};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use uuid::Uuid;

mod dashboard;
mod db;
mod proxy;

const SCHEMA_VERSION: u32 = 1;
const VERSION: &str = env!("CARGO_PKG_VERSION");

pyo3::create_exception!(agentledger, AgentLedgerError, PyRuntimeError);
pyo3::create_exception!(agentledger, ConfigError, AgentLedgerError);
pyo3::create_exception!(agentledger, CaptureError, AgentLedgerError);
pyo3::create_exception!(agentledger, StorageError, AgentLedgerError);
pyo3::create_exception!(agentledger, ReplayError, AgentLedgerError);
pyo3::create_exception!(agentledger, ProviderError, AgentLedgerError);
pyo3::create_exception!(agentledger, SecurityError, AgentLedgerError);

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("capture error: {0}")]
    Capture(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("security error: {0}")]
    Security(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    TomlSer(#[from] toml::ser::Error),
}

type Result<T> = std::result::Result<T, Error>;

fn to_py_err(err: Error) -> PyErr {
    match err {
        Error::Config(message) => ConfigError::new_err(message),
        Error::Capture(message) => CaptureError::new_err(message),
        Error::Storage(message) => StorageError::new_err(message),
        Error::Security(message) => SecurityError::new_err(message),
        Error::Unsupported(message) => AgentLedgerError::new_err(message),
        Error::Io(err) => StorageError::new_err(err.to_string()),
        Error::Json(err) => StorageError::new_err(err.to_string()),
        Error::TomlSer(err) => ConfigError::new_err(err.to_string()),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct AgentLedgerConfig {
    storage: StorageConfig,
    privacy: PrivacyConfig,
    proxy: ProxyConfig,
    agents: BTreeMap<String, AgentConfig>,
    providers: BTreeMap<String, ProviderConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StorageConfig {
    root: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PrivacyConfig {
    capture_prompts: bool,
    capture_diffs: bool,
    redact_env: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProxyConfig {
    enabled: bool,
    bind: String,
    record_bodies: bool,
    allowed_hosts: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AgentConfig {
    command: Vec<String>,
    env: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProviderConfig {
    kind: String,
    base_url: Option<String>,
    api_key_env: Option<String>,
    context_window: Option<u64>,
}

impl Default for AgentLedgerConfig {
    fn default() -> Self {
        let mut agents = BTreeMap::new();
        for (id, command) in [
            ("codex", vec!["codex"]),
            ("claude-code", vec!["claude"]),
            ("opencode", vec!["opencode"]),
            ("custom", vec![]),
        ] {
            agents.insert(
                id.to_string(),
                AgentConfig {
                    command: command.into_iter().map(str::to_string).collect(),
                    env: BTreeMap::new(),
                },
            );
        }

        let mut providers = BTreeMap::new();
        for (id, base_url, api_key_env) in [
            (
                "openrouter",
                Some("https://openrouter.ai/api/v1"),
                Some("OPENROUTER_API_KEY"),
            ),
            ("ollama", Some("http://127.0.0.1:11434/v1"), None),
            ("vllm", Some("http://127.0.0.1:8000/v1"), None),
            ("lm-studio", Some("http://127.0.0.1:1234/v1"), None),
            ("openai-compatible", None, None),
        ] {
            providers.insert(
                id.to_string(),
                ProviderConfig {
                    kind: "openai-compatible".to_string(),
                    base_url: base_url.map(str::to_string),
                    api_key_env: api_key_env.map(str::to_string),
                    context_window: None,
                },
            );
        }

        Self {
            storage: StorageConfig {
                root: ".agentledger".to_string(),
            },
            privacy: PrivacyConfig {
                capture_prompts: true,
                capture_diffs: true,
                redact_env: vec![
                    "API_KEY".to_string(),
                    "TOKEN".to_string(),
                    "SECRET".to_string(),
                    "PASSWORD".to_string(),
                    "AUTHORIZATION".to_string(),
                ],
            },
            proxy: ProxyConfig {
                enabled: false,
                bind: "127.0.0.1:0".to_string(),
                record_bodies: false,
                allowed_hosts: vec!["127.0.0.1".to_string(), "localhost".to_string()],
            },
            agents,
            providers,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LedgerEvent {
    record_type: String,
    schema_version: u32,
    previous_hash: Option<String>,
    hash: String,
    run: RunRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunRecord {
    id: String,
    task: String,
    agent: String,
    command: Vec<String>,
    repo: String,
    started_at: String,
    ended_at: String,
    duration_ms: u128,
    exit_code: Option<i32>,
    status: String,
    source_precision: String,
    stdout_path: String,
    stderr_path: String,
    stdout_preview: String,
    stderr_preview: String,
    git: GitSnapshot,
    evals: Vec<EvalRecord>,
    metrics: RunMetrics,
    #[serde(default)]
    llm_error_calls: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GitSnapshot {
    is_git_repo: bool,
    base_commit: Option<String>,
    dirty_before: bool,
    dirty_after: bool,
    diffstat: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EvalRecord {
    command: String,
    exit_code: Option<i32>,
    duration_ms: u128,
    status: String,
    stdout_preview: String,
    stderr_preview: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RunMetrics {
    llm_metrics_precision: String,
    token_input: Option<u64>,
    token_output: Option<u64>,
    token_total: Option<u64>,
    context_window: Option<u64>,
    context_used_ratio: Option<f64>,
    cost_usd: Option<f64>,
    ttft_ms: Option<u128>,
    output_tokens_per_second: Option<f64>,
}

#[derive(Debug, Clone)]
struct ProxyRunConfig {
    bind: String,
    upstream: String,
    api_key_env: Option<String>,
    record_bodies: bool,
    fail_on_llm_error: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct CompareReport {
    schema_version: u32,
    task: Option<String>,
    run_count: usize,
    runs: Vec<RunSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunSummary {
    id: String,
    task: String,
    agent: String,
    status: String,
    duration_ms: u128,
    exit_code: Option<i32>,
    eval_status: String,
    repo: String,
    started_at: String,
    cost_usd: Option<f64>,
    token_total: Option<u64>,
    llm_metrics_precision: String,
    llm_call_count: u64,
}

#[derive(Parser, Debug)]
#[command(name = "agentledger")]
#[command(version = VERSION)]
#[command(about = "Local-first benchmark ledger for coding agents and LLM providers")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Init {
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    Run {
        #[arg(long)]
        task: String,
        #[arg(long, default_value = "custom")]
        agent: String,
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        allow_dirty: bool,
        #[arg(long = "eval")]
        eval_commands: Vec<String>,
        #[arg(long)]
        proxy_upstream: Option<String>,
        #[arg(long, default_value = "127.0.0.1:0")]
        proxy_bind: String,
        #[arg(long)]
        proxy_api_key_env: Option<String>,
        #[arg(long)]
        proxy_record_bodies: bool,
        #[arg(long)]
        fail_on_llm_error: bool,
        #[arg(last = true, required = true)]
        command: Vec<OsString>,
    },
    Bench {
        #[arg(long)]
        matrix: PathBuf,
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        task: Option<String>,
    },
    Compare {
        task: Option<String>,
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    Replay {
        run_id: String,
        #[arg(long, default_value = "rerun")]
        mode: String,
    },
    Eval {
        run_id: String,
        #[arg(long = "test", required = true)]
        tests: Vec<String>,
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    Dashboard {
        #[arg(long, default_value = "127.0.0.1:0")]
        bind: String,
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    Proxy {
        #[arg(long, default_value = "127.0.0.1:0")]
        bind: String,
        #[arg(long)]
        upstream: String,
        #[arg(long, default_value = ".")]
        root: PathBuf,
        #[arg(long)]
        api_key_env: Option<String>,
        #[arg(long)]
        record_bodies: bool,
    },
    Export {
        #[arg(long, default_value = "jsonl")]
        format: String,
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    Providers {
        #[command(subcommand)]
        command: RegistryCommand,
    },
    Agents {
        #[command(subcommand)]
        command: RegistryCommand,
    },
    Doctor {
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    Db {
        #[command(subcommand)]
        command: DbCommand,
    },
}

#[derive(Subcommand, Debug)]
enum RegistryCommand {
    List,
    Doctor,
}

#[derive(Subcommand, Debug)]
enum DbCommand {
    Sync {
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    Query {
        sql: String,
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
}

#[pyfunction]
fn version() -> &'static str {
    VERSION
}

#[pyfunction]
fn init_project(path: String) -> PyResult<String> {
    init(Path::new(&path)).map_err(to_py_err)
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn run_task(
    py: Python<'_>,
    task: String,
    agent: String,
    command: Vec<String>,
    repo: Option<String>,
    eval_commands: Option<Vec<String>>,
    allow_dirty: Option<bool>,
    proxy_upstream: Option<String>,
    proxy_bind: Option<String>,
    proxy_api_key_env: Option<String>,
    proxy_record_bodies: Option<bool>,
    fail_on_llm_error: Option<bool>,
) -> PyResult<String> {
    let repo = repo.unwrap_or_else(|| ".".to_string());
    let proxy_config = proxy_upstream.map(|upstream| ProxyRunConfig {
        bind: proxy_bind.unwrap_or_else(|| "127.0.0.1:0".to_string()),
        upstream,
        api_key_env: proxy_api_key_env,
        record_bodies: proxy_record_bodies.unwrap_or(false),
        fail_on_llm_error: fail_on_llm_error.unwrap_or(false),
    });
    // Release the GIL: the captured command or the proxy may call back into
    // Python-hosted servers running in other threads of this process.
    let record = py
        .detach(move || {
            run_capture(
                &task,
                &agent,
                command,
                Path::new(&repo),
                eval_commands.unwrap_or_default(),
                allow_dirty.unwrap_or(false),
                proxy_config,
            )
        })
        .map_err(to_py_err)?;
    serde_json::to_string_pretty(&record).map_err(|err| to_py_err(Error::Json(err)))
}

#[pyfunction]
fn bench_matrix(
    py: Python<'_>,
    matrix: String,
    repo: Option<String>,
    task: Option<String>,
) -> PyResult<String> {
    let repo = repo.unwrap_or_else(|| ".".to_string());
    let report = py
        .detach(move || run_bench(Path::new(&matrix), Path::new(&repo), task.as_deref()))
        .map_err(to_py_err)?;
    serde_json::to_string_pretty(&report).map_err(|err| to_py_err(Error::Json(err)))
}

#[pyfunction]
fn eval_run(
    py: Python<'_>,
    run_id: String,
    tests: Vec<String>,
    root: Option<String>,
) -> PyResult<String> {
    let root = root.unwrap_or_else(|| ".".to_string());
    let record = py
        .detach(move || run_post_eval(&run_id, &tests, Path::new(&root)))
        .map_err(to_py_err)?;
    serde_json::to_string_pretty(&record).map_err(|err| to_py_err(Error::Json(err)))
}

#[pyfunction]
fn sync_db(py: Python<'_>, root: Option<String>) -> PyResult<String> {
    let root = root.unwrap_or_else(|| ".".to_string());
    let report = py
        .detach(move || db::sync(Path::new(&root)))
        .map_err(|err| to_py_err(Error::Storage(err)))?;
    serde_json::to_string_pretty(&report).map_err(|err| to_py_err(Error::Json(err)))
}

#[pyfunction]
fn query_db(py: Python<'_>, sql: String, root: Option<String>) -> PyResult<String> {
    let root = root.unwrap_or_else(|| ".".to_string());
    let rows = py
        .detach(move || db::query(Path::new(&root), &sql))
        .map_err(|err| to_py_err(Error::Storage(err)))?;
    serde_json::to_string(&rows).map_err(|err| to_py_err(Error::Json(err)))
}

#[pyfunction]
fn compare_runs(task: Option<String>, root: Option<String>) -> PyResult<String> {
    let root = root.unwrap_or_else(|| ".".to_string());
    let report = compare(task, Path::new(&root)).map_err(to_py_err)?;
    serde_json::to_string_pretty(&report).map_err(|err| to_py_err(Error::Json(err)))
}

#[pyfunction]
fn export_ledger(format: String, output: Option<String>, root: Option<String>) -> PyResult<String> {
    let root = root.unwrap_or_else(|| ".".to_string());
    let output = output.map(PathBuf::from);
    export(&format, output.as_deref(), Path::new(&root)).map_err(to_py_err)
}

#[pyfunction]
fn doctor(root: Option<String>) -> PyResult<String> {
    let root = root.unwrap_or_else(|| ".".to_string());
    Ok(doctor_report(Path::new(&root)))
}

#[pyfunction]
fn start_proxy(
    py: Python<'_>,
    bind: String,
    upstream: String,
    root: Option<String>,
    api_key_env: Option<String>,
    record_bodies: Option<bool>,
) -> PyResult<()> {
    let root = root.unwrap_or_else(|| ".".to_string());
    let api_key = api_key_env.and_then(|name| std::env::var(name).ok());
    py.detach(move || {
        proxy::serve_proxy(
            &bind,
            &upstream,
            Path::new(&root),
            api_key,
            record_bodies.unwrap_or(false),
        )
    })
    .map_err(|err| to_py_err(Error::Capture(err)))
}

#[pyfunction]
fn run_cli(py: Python<'_>, argv: Vec<String>) -> PyResult<i32> {
    let args = std::iter::once("agentledger".to_string()).chain(argv);
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(err) => {
            let code = err.exit_code();
            err.print()
                .map_err(|io_err| PyRuntimeError::new_err(io_err.to_string()))?;
            return Ok(code);
        }
    };

    match py.detach(move || dispatch(cli)) {
        Ok(()) => Ok(0),
        Err(err) => {
            eprintln!("error: {err}");
            Ok(1)
        }
    }
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("AgentLedgerError", py.get_type::<AgentLedgerError>())?;
    m.add("ConfigError", py.get_type::<ConfigError>())?;
    m.add("CaptureError", py.get_type::<CaptureError>())?;
    m.add("StorageError", py.get_type::<StorageError>())?;
    m.add("ReplayError", py.get_type::<ReplayError>())?;
    m.add("ProviderError", py.get_type::<ProviderError>())?;
    m.add("SecurityError", py.get_type::<SecurityError>())?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(init_project, m)?)?;
    m.add_function(wrap_pyfunction!(run_task, m)?)?;
    m.add_function(wrap_pyfunction!(bench_matrix, m)?)?;
    m.add_function(wrap_pyfunction!(eval_run, m)?)?;
    m.add_function(wrap_pyfunction!(sync_db, m)?)?;
    m.add_function(wrap_pyfunction!(query_db, m)?)?;
    m.add_function(wrap_pyfunction!(compare_runs, m)?)?;
    m.add_function(wrap_pyfunction!(export_ledger, m)?)?;
    m.add_function(wrap_pyfunction!(doctor, m)?)?;
    m.add_function(wrap_pyfunction!(start_proxy, m)?)?;
    m.add_function(wrap_pyfunction!(run_cli, m)?)?;
    Ok(())
}

fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Init { path } => {
            println!("{}", init(&path)?);
        }
        Commands::Run {
            task,
            agent,
            repo,
            allow_dirty,
            eval_commands,
            proxy_upstream,
            proxy_bind,
            proxy_api_key_env,
            proxy_record_bodies,
            fail_on_llm_error,
            command,
        } => {
            let command = command
                .into_iter()
                .map(|part| part.to_string_lossy().to_string())
                .collect::<Vec<_>>();
            let proxy_config = proxy_upstream.map(|upstream| ProxyRunConfig {
                bind: proxy_bind,
                upstream,
                api_key_env: proxy_api_key_env,
                record_bodies: proxy_record_bodies,
                fail_on_llm_error,
            });
            let record = run_capture(
                &task,
                &agent,
                command,
                &repo,
                eval_commands,
                allow_dirty,
                proxy_config,
            )?;
            println!("{}", serde_json::to_string_pretty(&record)?);
        }
        Commands::Bench { matrix, repo, task } => {
            let report = run_bench(&matrix, &repo, task.as_deref())?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::Compare { task, root } => {
            let report = compare(task, &root)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::Replay { run_id, mode } => {
            return Err(Error::Unsupported(format!(
                "replay mode '{mode}' for run '{run_id}' is planned after proxy capture"
            )));
        }
        Commands::Eval {
            run_id,
            tests,
            root,
        } => {
            let record = run_post_eval(&run_id, &tests, &root)?;
            println!("{}", serde_json::to_string_pretty(&record)?);
        }
        Commands::Dashboard { bind, root } => {
            dashboard::serve_dashboard(&bind, &root).map_err(Error::Capture)?;
        }
        Commands::Proxy {
            bind,
            upstream,
            root,
            api_key_env,
            record_bodies,
        } => {
            let api_key = api_key_env.and_then(|name| std::env::var(name).ok());
            proxy::serve_proxy(&bind, &upstream, &root, api_key, record_bodies)
                .map_err(Error::Capture)?;
        }
        Commands::Export {
            format,
            output,
            root,
        } => {
            println!("{}", export(&format, output.as_deref(), &root)?);
        }
        Commands::Providers { command } => match command {
            RegistryCommand::List => {
                println!("openrouter\nollama\nvllm\nlm-studio\nopenai-compatible");
            }
            RegistryCommand::Doctor => {
                println!("{}", provider_doctor());
            }
        },
        Commands::Agents { command } => match command {
            RegistryCommand::List => {
                println!("codex\nclaude-code\nopencode\ncustom");
            }
            RegistryCommand::Doctor => {
                println!("{}", agent_doctor());
            }
        },
        Commands::Doctor { root } => {
            println!("{}", doctor_report(&root));
        }
        Commands::Db { command } => match command {
            DbCommand::Sync { root } => {
                let report = db::sync(&root).map_err(Error::Storage)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            DbCommand::Query { sql, root } => {
                for row in db::query(&root, &sql).map_err(Error::Storage)? {
                    println!("{}", serde_json::to_string(&row)?);
                }
            }
        },
    }
    Ok(())
}

fn init(path: &Path) -> Result<String> {
    fs::create_dir_all(path)?;
    let ledger_dir = path.join(".agentledger");
    fs::create_dir_all(&ledger_dir)?;
    secure_dir(&ledger_dir)?;

    let runs_dir = ledger_dir.join("runs");
    fs::create_dir_all(&runs_dir)?;
    secure_dir(&runs_dir)?;

    let config_path = path.join("AgentLedger.toml");
    if !config_path.exists() {
        let config = AgentLedgerConfig::default();
        let rendered = toml::to_string_pretty(&config)?;
        fs::write(&config_path, rendered)?;
    }

    let events_path = ledger_dir.join("events.ndjson");
    if !events_path.exists() {
        File::create(events_path)?;
    }

    Ok(format!(
        "Initialized AgentLedger at {}",
        ledger_dir.to_string_lossy()
    ))
}

fn run_capture(
    task: &str,
    agent: &str,
    command: Vec<String>,
    repo: &Path,
    eval_commands: Vec<String>,
    allow_dirty: bool,
    proxy_config: Option<ProxyRunConfig>,
) -> Result<RunRecord> {
    if command.is_empty() {
        return Err(Error::Config("run command cannot be empty".to_string()));
    }

    let repo = repo
        .canonicalize()
        .map_err(|err| Error::Config(format!("cannot resolve repo path: {err}")))?;
    init(&repo)?;

    let git_before = git_snapshot(&repo, false);
    if git_before.dirty_before && !allow_dirty {
        return Err(Error::Security(
            "repository is dirty; commit changes or pass --allow-dirty".to_string(),
        ));
    }

    let run_id = Uuid::new_v4().to_string();
    let ledger_dir = repo.join(".agentledger");
    let run_dir = ledger_dir.join("runs").join(&run_id);
    fs::create_dir_all(&run_dir)?;
    secure_dir(&run_dir)?;

    let stdout_path = run_dir.join("stdout.txt");
    let stderr_path = run_dir.join("stderr.txt");

    let proxy_api_key_env_present = proxy_config
        .as_ref()
        .and_then(|config| config.api_key_env.as_ref())
        .is_some();
    let proxy_handle = if let Some(config) = proxy_config.as_ref() {
        let api_key = config
            .api_key_env
            .as_ref()
            .and_then(|name| std::env::var(name).ok());
        Some(
            proxy::start_proxy_background(
                &config.bind,
                &config.upstream,
                &repo,
                api_key,
                config.record_bodies,
                Some(run_id.clone()),
            )
            .map_err(Error::Capture)?,
        )
    } else {
        None
    };

    let started_at = now_rfc3339()?;
    let timer = Instant::now();
    let mut child = Command::new(&command[0]);
    child
        .args(&command[1..])
        .current_dir(&repo)
        .env("AGENTLEDGER_RUN_ID", &run_id)
        .env("AGENTLEDGER_ROOT", ledger_dir.to_string_lossy().to_string())
        .env(
            "AGENTLEDGER_PROXY_RUN_HEADER",
            format!("x-agentledger-run-id: {run_id}"),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(proxy_handle) = proxy_handle.as_ref() {
        child
            .env("AGENTLEDGER_PROXY_URL", proxy_handle.base_url())
            .env("OPENAI_BASE_URL", proxy_handle.base_url())
            .env("OPENAI_API_BASE", proxy_handle.base_url());
        if proxy_api_key_env_present || std::env::var_os("OPENAI_API_KEY").is_none() {
            child.env("OPENAI_API_KEY", "agentledger-proxy");
        }
    }

    let output = child
        .output()
        .map_err(|err| Error::Capture(format!("failed to run {:?}: {err}", command)))?;
    // Dropping the handle joins the proxy thread, so the error counter is
    // final once the drop returns.
    let llm_error_counter = proxy_handle
        .as_ref()
        .map(|handle| handle.error_calls_handle());
    drop(proxy_handle);
    let llm_error_calls = llm_error_counter
        .map(|counter| counter.load(std::sync::atomic::Ordering::SeqCst))
        .unwrap_or(0);
    let duration_ms = timer.elapsed().as_millis();
    let ended_at = now_rfc3339()?;

    fs::write(&stdout_path, &output.stdout)?;
    fs::write(&stderr_path, &output.stderr)?;

    let git_after = git_snapshot(&repo, true);
    let evals = eval_commands
        .iter()
        .map(|cmd| run_eval(cmd, &repo))
        .collect::<Result<Vec<_>>>()?;

    let eval_failed = evals.iter().any(|eval| eval.status != "passed");
    let fail_on_llm_error = proxy_config
        .as_ref()
        .is_some_and(|config| config.fail_on_llm_error);
    let llm_failed = fail_on_llm_error && llm_error_calls > 0;
    let status = if output.status.success() && !eval_failed && !llm_failed {
        "passed"
    } else {
        "failed"
    }
    .to_string();

    let record = RunRecord {
        id: run_id,
        task: task.to_string(),
        agent: agent.to_string(),
        command,
        repo: repo.to_string_lossy().to_string(),
        started_at,
        ended_at,
        duration_ms,
        exit_code: output.status.code(),
        status,
        source_precision: "observed".to_string(),
        stdout_path: stdout_path.to_string_lossy().to_string(),
        stderr_path: stderr_path.to_string_lossy().to_string(),
        stdout_preview: preview_bytes(&output.stdout),
        stderr_preview: preview_bytes(&output.stderr),
        git: GitSnapshot {
            dirty_after: git_after.dirty_after,
            diffstat: git_after.diffstat,
            ..git_before
        },
        evals,
        metrics: RunMetrics {
            llm_metrics_precision: "unknown".to_string(),
            token_input: None,
            token_output: None,
            token_total: None,
            context_window: None,
            context_used_ratio: None,
            cost_usd: None,
            ttft_ms: None,
            output_tokens_per_second: None,
        },
        llm_error_calls,
    };

    append_run_event(&ledger_dir, &record)?;
    Ok(record)
}

fn run_eval(command: &str, repo: &Path) -> Result<EvalRecord> {
    let timer = Instant::now();
    let output = shell_command(command)
        .current_dir(repo)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| Error::Capture(format!("failed to run eval '{command}': {err}")))?;
    let duration_ms = timer.elapsed().as_millis();
    Ok(EvalRecord {
        command: command.to_string(),
        exit_code: output.status.code(),
        duration_ms,
        status: if output.status.success() {
            "passed".to_string()
        } else {
            "failed".to_string()
        },
        stdout_preview: preview_bytes(&output.stdout),
        stderr_preview: preview_bytes(&output.stderr),
    })
}

#[derive(Debug, Deserialize)]
struct BenchMatrix {
    #[serde(default = "default_repeats")]
    repeats: u32,
    #[serde(default)]
    allow_dirty: bool,
    #[serde(default)]
    fail_on_llm_error: bool,
    tasks: Vec<BenchTask>,
    agents: Vec<BenchAgent>,
    #[serde(default)]
    providers: Vec<BenchProvider>,
}

fn default_repeats() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
struct BenchTask {
    name: String,
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    evals: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct BenchAgent {
    name: String,
    command: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct BenchProvider {
    name: String,
    upstream: String,
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default = "default_proxy_bind")]
    bind: String,
    #[serde(default)]
    record_bodies: bool,
}

fn default_proxy_bind() -> String {
    "127.0.0.1:0".to_string()
}

#[derive(Debug, Serialize)]
struct BenchReport {
    schema_version: u32,
    matrix: String,
    cell_count: usize,
    passed: usize,
    failed: usize,
    cells: Vec<BenchCell>,
}

#[derive(Debug, Serialize)]
struct BenchCell {
    task: String,
    agent: String,
    provider: Option<String>,
    repeat: u32,
    run_id: Option<String>,
    status: String,
    duration_ms: Option<u128>,
    error: Option<String>,
}

fn parse_bench_matrix(raw: &str) -> Result<BenchMatrix> {
    let matrix: BenchMatrix =
        toml::from_str(raw).map_err(|err| Error::Config(format!("invalid bench matrix: {err}")))?;
    if matrix.tasks.is_empty() {
        return Err(Error::Config(
            "bench matrix needs at least one [[tasks]] entry".to_string(),
        ));
    }
    if matrix.agents.is_empty() {
        return Err(Error::Config(
            "bench matrix needs at least one [[agents]] entry".to_string(),
        ));
    }
    for agent in &matrix.agents {
        if agent.command.is_empty() {
            return Err(Error::Config(format!(
                "bench agent '{}' has an empty command",
                agent.name
            )));
        }
    }
    Ok(matrix)
}

fn bench_command(template: &[String], task: &BenchTask) -> Vec<String> {
    template
        .iter()
        .map(|part| {
            part.replace("{prompt}", &task.prompt)
                .replace("{task}", &task.name)
        })
        .collect()
}

fn run_bench(matrix_path: &Path, repo: &Path, task_filter: Option<&str>) -> Result<BenchReport> {
    let raw = fs::read_to_string(matrix_path).map_err(|err| {
        Error::Config(format!(
            "cannot read bench matrix {}: {err}",
            matrix_path.display()
        ))
    })?;
    let matrix = parse_bench_matrix(&raw)?;
    let tasks = matrix
        .tasks
        .iter()
        .filter(|task| task_filter.is_none_or(|filter| task.name == filter))
        .collect::<Vec<_>>();
    if tasks.is_empty() {
        return Err(Error::Config(format!(
            "no bench task matches filter '{}'",
            task_filter.unwrap_or_default()
        )));
    }
    let providers = if matrix.providers.is_empty() {
        vec![None]
    } else {
        matrix.providers.iter().map(Some).collect::<Vec<_>>()
    };
    let repeats = matrix.repeats.max(1);

    let mut cells = Vec::new();
    for task in &tasks {
        for agent in &matrix.agents {
            for provider in &providers {
                let agent_label = match provider {
                    Some(provider) => format!("{}@{}", agent.name, provider.name),
                    None => agent.name.clone(),
                };
                for repeat in 1..=repeats {
                    eprintln!(
                        "bench: task={} agent={} repeat={repeat}/{repeats}",
                        task.name, agent_label
                    );
                    let command = bench_command(&agent.command, task);
                    let proxy_config = provider.map(|provider| ProxyRunConfig {
                        bind: provider.bind.clone(),
                        upstream: provider.upstream.clone(),
                        api_key_env: provider.api_key_env.clone(),
                        record_bodies: provider.record_bodies,
                        fail_on_llm_error: matrix.fail_on_llm_error,
                    });
                    let cell = match run_capture(
                        &task.name,
                        &agent_label,
                        command,
                        repo,
                        task.evals.clone(),
                        matrix.allow_dirty,
                        proxy_config,
                    ) {
                        Ok(record) => BenchCell {
                            task: task.name.clone(),
                            agent: agent_label.clone(),
                            provider: provider.map(|provider| provider.name.clone()),
                            repeat,
                            run_id: Some(record.id),
                            status: record.status,
                            duration_ms: Some(record.duration_ms),
                            error: None,
                        },
                        Err(err) => BenchCell {
                            task: task.name.clone(),
                            agent: agent_label.clone(),
                            provider: provider.map(|provider| provider.name.clone()),
                            repeat,
                            run_id: None,
                            status: "error".to_string(),
                            duration_ms: None,
                            error: Some(err.to_string()),
                        },
                    };
                    cells.push(cell);
                }
            }
        }
    }

    let passed = cells.iter().filter(|cell| cell.status == "passed").count();
    Ok(BenchReport {
        schema_version: SCHEMA_VERSION,
        matrix: matrix_path.display().to_string(),
        cell_count: cells.len(),
        passed,
        failed: cells.len() - passed,
        cells,
    })
}

fn shell_command(command: &str) -> Command {
    #[cfg(windows)]
    {
        let mut cmd = Command::new("cmd");
        cmd.arg("/C").arg(command);
        cmd
    }
    #[cfg(not(windows))]
    {
        let mut cmd = Command::new("sh");
        cmd.arg("-lc").arg(command);
        cmd
    }
}

fn git_snapshot(repo: &Path, after: bool) -> GitSnapshot {
    let is_git_repo = git_output(repo, &["rev-parse", "--is-inside-work-tree"])
        .map(|value| value.trim() == "true")
        .unwrap_or(false);

    if !is_git_repo {
        return GitSnapshot {
            is_git_repo: false,
            base_commit: None,
            dirty_before: false,
            dirty_after: false,
            diffstat: None,
        };
    }

    let base_commit = git_output(repo, &["rev-parse", "HEAD"])
        .ok()
        .map(|value| value.trim().to_string());
    let status = git_output(repo, &["status", "--porcelain"]).unwrap_or_default();
    let dirty = status.lines().any(|line| !is_ledger_artifact_status(line));
    let diffstat = if after {
        git_output(repo, &["diff", "--stat"]).ok()
    } else {
        None
    };

    GitSnapshot {
        is_git_repo: true,
        base_commit,
        dirty_before: dirty,
        dirty_after: dirty,
        diffstat,
    }
}

fn is_ledger_artifact_status(line: &str) -> bool {
    let path = line.get(3..).unwrap_or(line).trim();
    path == "AgentLedger.toml" || path == ".agentledger/" || path.starts_with(".agentledger/")
}

fn git_output(repo: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;
    if !output.status.success() {
        return Err(Error::Capture(preview_bytes(&output.stderr)));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn append_run_event(ledger_dir: &Path, record: &RunRecord) -> Result<()> {
    let events_path = ledger_dir.join("events.ndjson");
    let previous_hash = last_hash(&events_path)?;
    let payload = serde_json::json!({
        "record_type": "run",
        "schema_version": SCHEMA_VERSION,
        "previous_hash": previous_hash,
        "run": record,
    });
    let hash = blake3::hash(serde_json::to_vec(&payload)?.as_slice())
        .to_hex()
        .to_string();
    let event = LedgerEvent {
        record_type: "run".to_string(),
        schema_version: SCHEMA_VERSION,
        previous_hash: payload
            .get("previous_hash")
            .and_then(|value| value.as_str())
            .map(str::to_string),
        hash,
        run: record.clone(),
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&events_path)?;
    writeln!(file, "{}", serde_json::to_string(&event)?)?;
    Ok(())
}

fn last_hash(events_path: &Path) -> Result<Option<String>> {
    if !events_path.exists() {
        return Ok(None);
    }
    let file = File::open(events_path)?;
    let reader = BufReader::new(file);
    let mut last = None;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: LedgerEvent = serde_json::from_str(&line)?;
        last = Some(event.hash);
    }
    Ok(last)
}

fn load_runs(root: &Path) -> Result<Vec<RunRecord>> {
    let events_path = root.join(".agentledger").join("events.ndjson");
    if !events_path.exists() {
        return Ok(vec![]);
    }
    let file = File::open(events_path)?;
    let reader = BufReader::new(file);
    // The ledger is append-only: a re-evaluated run is appended as a new
    // event with the same id, and the latest event wins here.
    let mut order = Vec::new();
    let mut by_id = BTreeMap::<String, RunRecord>::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: LedgerEvent = serde_json::from_str(&line)?;
        if !by_id.contains_key(&event.run.id) {
            order.push(event.run.id.clone());
        }
        by_id.insert(event.run.id.clone(), event.run);
    }
    Ok(order
        .into_iter()
        .filter_map(|id| by_id.remove(&id))
        .collect())
}

fn run_post_eval(run_id: &str, tests: &[String], root: &Path) -> Result<RunRecord> {
    if tests.is_empty() {
        return Err(Error::Config(
            "pass at least one --test command".to_string(),
        ));
    }
    let mut run = load_runs(root)?
        .into_iter()
        .find(|run| run.id == run_id)
        .ok_or_else(|| Error::Storage(format!("run '{run_id}' not found in ledger")))?;
    let repo = PathBuf::from(&run.repo);
    if !repo.is_dir() {
        return Err(Error::Capture(format!(
            "recorded repo path '{}' no longer exists",
            run.repo
        )));
    }

    for test in tests {
        run.evals.push(run_eval(test, &repo)?);
    }
    let command_passed = run.exit_code == Some(0);
    let evals_passed = run.evals.iter().all(|eval| eval.status == "passed");
    run.status = if command_passed && evals_passed {
        "passed"
    } else {
        "failed"
    }
    .to_string();

    append_run_event(&root.join(".agentledger"), &run)?;
    Ok(run)
}

#[derive(Debug, Default)]
struct LlmMetricAggregate {
    call_count: u64,
    duration_ms: u128,
    metrics: RunMetrics,
}

#[derive(Debug, Deserialize)]
struct LlmCallLine {
    run_id: Option<String>,
    duration_ms: Option<u128>,
    metrics: LlmCallLineMetrics,
}

#[derive(Debug, Deserialize)]
struct LlmCallLineMetrics {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    cost_usd: Option<f64>,
    ttft_ms: Option<u128>,
}

fn load_llm_metric_aggregates(root: &Path) -> Result<BTreeMap<String, LlmMetricAggregate>> {
    let calls_path = root.join(".agentledger").join("llm_calls.ndjson");
    if !calls_path.exists() {
        return Ok(BTreeMap::new());
    }

    let file = File::open(calls_path)?;
    let reader = BufReader::new(file);
    let mut aggregates = BTreeMap::<String, LlmMetricAggregate>::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let call: LlmCallLine = serde_json::from_str(&line)?;
        let Some(run_id) = call.run_id else {
            continue;
        };
        let aggregate = aggregates.entry(run_id).or_default();
        aggregate.call_count += 1;
        aggregate.duration_ms += call.duration_ms.unwrap_or_default();
        aggregate.metrics.llm_metrics_precision = "exact".to_string();
        add_u64(
            &mut aggregate.metrics.token_input,
            call.metrics.input_tokens,
        );
        add_u64(
            &mut aggregate.metrics.token_output,
            call.metrics.output_tokens,
        );
        add_u64(
            &mut aggregate.metrics.token_total,
            call.metrics.total_tokens,
        );
        add_f64(&mut aggregate.metrics.cost_usd, call.metrics.cost_usd);
        aggregate.metrics.ttft_ms = min_u128(aggregate.metrics.ttft_ms, call.metrics.ttft_ms);
    }

    for aggregate in aggregates.values_mut() {
        if let Some(output_tokens) = aggregate.metrics.token_output {
            if aggregate.duration_ms > 0 {
                aggregate.metrics.output_tokens_per_second =
                    Some(output_tokens as f64 / (aggregate.duration_ms as f64 / 1000.0));
            }
        }
    }

    Ok(aggregates)
}

fn merge_run_metrics(base: &RunMetrics, aggregate: Option<&LlmMetricAggregate>) -> RunMetrics {
    let Some(aggregate) = aggregate else {
        return base.clone();
    };
    let mut metrics = base.clone();
    metrics.llm_metrics_precision = "exact".to_string();
    metrics.token_input = aggregate.metrics.token_input;
    metrics.token_output = aggregate.metrics.token_output;
    metrics.token_total = aggregate.metrics.token_total;
    metrics.cost_usd = aggregate.metrics.cost_usd;
    metrics.ttft_ms = aggregate.metrics.ttft_ms;
    metrics.output_tokens_per_second = aggregate.metrics.output_tokens_per_second;
    metrics
}

fn add_u64(target: &mut Option<u64>, value: Option<u64>) {
    if let Some(value) = value {
        *target = Some(target.unwrap_or_default() + value);
    }
}

fn add_f64(target: &mut Option<f64>, value: Option<f64>) {
    if let Some(value) = value {
        *target = Some(target.unwrap_or_default() + value);
    }
}

fn min_u128(target: Option<u128>, value: Option<u128>) -> Option<u128> {
    match (target, value) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (None, Some(value)) => Some(value),
        (value, None) => value,
    }
}

fn compare(task: Option<String>, root: &Path) -> Result<CompareReport> {
    let mut runs = load_runs(root)?;
    let llm_aggregates = load_llm_metric_aggregates(root)?;
    if let Some(task) = &task {
        runs.retain(|run| &run.task == task);
    }
    let summaries = runs
        .into_iter()
        .map(|run| {
            let eval_status = if run.evals.is_empty() {
                "not_run".to_string()
            } else if run.evals.iter().all(|eval| eval.status == "passed") {
                "passed".to_string()
            } else {
                "failed".to_string()
            };
            let aggregate = llm_aggregates.get(&run.id);
            let metrics = merge_run_metrics(&run.metrics, aggregate);
            RunSummary {
                id: run.id,
                task: run.task,
                agent: run.agent,
                status: run.status,
                duration_ms: run.duration_ms,
                exit_code: run.exit_code,
                eval_status,
                repo: run.repo,
                started_at: run.started_at,
                cost_usd: metrics.cost_usd,
                token_total: metrics.token_total,
                llm_metrics_precision: metrics.llm_metrics_precision,
                llm_call_count: aggregate.map_or(0, |aggregate| aggregate.call_count),
            }
        })
        .collect::<Vec<_>>();
    Ok(CompareReport {
        schema_version: SCHEMA_VERSION,
        task,
        run_count: summaries.len(),
        runs: summaries,
    })
}

fn export(format: &str, output: Option<&Path>, root: &Path) -> Result<String> {
    match format {
        "jsonl" => export_jsonl(output, root),
        "csv" => export_csv(output, root),
        "parquet" => Err(Error::Unsupported(
            "parquet export is planned for the analytics extra; use jsonl or csv in the MVP"
                .to_string(),
        )),
        "otlp" => Err(Error::Unsupported(
            "OTLP export is planned after OpenTelemetry span capture lands".to_string(),
        )),
        other => Err(Error::Config(format!(
            "unsupported export format '{other}', expected jsonl, csv, parquet, or otlp"
        ))),
    }
}

fn export_jsonl(output: Option<&Path>, root: &Path) -> Result<String> {
    let source = root.join(".agentledger").join("events.ndjson");
    if !source.exists() {
        return Err(Error::Storage(format!(
            "ledger does not exist at {}",
            source.display()
        )));
    }
    let target = output
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".agentledger").join("export.jsonl"));
    fs::copy(&source, &target)?;
    Ok(format!("Exported JSONL to {}", target.display()))
}

fn export_csv(output: Option<&Path>, root: &Path) -> Result<String> {
    let runs = load_runs(root)?;
    let target = output
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".agentledger").join("runs.csv"));
    let mut file = File::create(&target)?;
    writeln!(
        file,
        "id,task,agent,status,duration_ms,exit_code,eval_status,started_at,repo"
    )?;
    for run in runs {
        let eval_status = if run.evals.is_empty() {
            "not_run".to_string()
        } else if run.evals.iter().all(|eval| eval.status == "passed") {
            "passed".to_string()
        } else {
            "failed".to_string()
        };
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{}",
            csv_escape(&run.id),
            csv_escape(&run.task),
            csv_escape(&run.agent),
            csv_escape(&run.status),
            run.duration_ms,
            run.exit_code
                .map(|code| code.to_string())
                .unwrap_or_default(),
            csv_escape(&eval_status),
            csv_escape(&run.started_at),
            csv_escape(&run.repo),
        )?;
    }
    Ok(format!("Exported CSV to {}", target.display()))
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn doctor_report(root: &Path) -> String {
    let mut lines = Vec::new();
    lines.push(format!("AgentLedger {VERSION}"));
    lines.push(format!("root: {}", root.display()));
    lines.push(format!(
        "ledger: {}",
        if root.join(".agentledger").exists() {
            "present"
        } else {
            "missing; run agentledger init"
        }
    ));
    lines.push(format!("git: {}", command_status("git")));
    lines.push(format!("agents:\n{}", indent(&agent_doctor())));
    lines.push(format!("providers:\n{}", indent(&provider_doctor())));
    lines.join("\n")
}

fn agent_doctor() -> String {
    [
        ("codex", "codex"),
        ("claude-code", "claude"),
        ("opencode", "opencode"),
    ]
    .iter()
    .map(|(name, bin)| format!("{name}: {}", command_status(bin)))
    .collect::<Vec<_>>()
    .join("\n")
}

fn provider_doctor() -> String {
    [
        ("openrouter", "OPENROUTER_API_KEY optional for live calls"),
        ("ollama", "http://127.0.0.1:11434/v1"),
        ("vllm", "http://127.0.0.1:8000/v1"),
        ("lm-studio", "http://127.0.0.1:1234/v1"),
        ("openai-compatible", "custom base URL"),
    ]
    .iter()
    .map(|(name, detail)| format!("{name}: configured ({detail})"))
    .collect::<Vec<_>>()
    .join("\n")
}

fn command_status(name: &str) -> String {
    let path_var = match std::env::var_os("PATH") {
        Some(value) => value,
        None => return "missing PATH".to_string(),
    };
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return format!("found at {}", candidate.display());
        }
        #[cfg(windows)]
        {
            let candidate = dir.join(format!("{name}.exe"));
            if candidate.is_file() {
                return format!("found at {}", candidate.display());
            }
        }
    }
    "not found".to_string()
}

fn indent(value: &str) -> String {
    value
        .lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn preview_bytes(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut preview = text.chars().take(2000).collect::<String>();
    if text.chars().count() > 2000 {
        preview.push_str("\n[truncated]");
    }
    preview
}

fn now_rfc3339() -> Result<String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|err| Error::Storage(err.to_string()))
}

#[cfg(unix)]
fn secure_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o700);
    fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn secure_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> PathBuf {
        let root = std::env::temp_dir().join(format!("agentledger-test-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join(".agentledger")).expect("create test ledger");
        root
    }

    #[test]
    fn default_config_roundtrips_through_toml() {
        let config = AgentLedgerConfig::default();
        let rendered = toml::to_string_pretty(&config).expect("serialize config");
        let parsed: AgentLedgerConfig = toml::from_str(&rendered).expect("parse config");
        assert_eq!(parsed.storage.root, ".agentledger");
        assert!(!parsed.proxy.enabled);
        assert!(parsed.agents.contains_key("claude-code"));
        assert_eq!(
            parsed.providers["ollama"].base_url.as_deref(),
            Some("http://127.0.0.1:11434/v1")
        );
    }

    #[test]
    fn ledger_artifacts_are_ignored_in_git_status() {
        assert!(is_ledger_artifact_status("?? AgentLedger.toml"));
        assert!(is_ledger_artifact_status("?? .agentledger/"));
        assert!(is_ledger_artifact_status(
            " M .agentledger/runs/x/stdout.txt"
        ));
        assert!(!is_ledger_artifact_status(" M src/main.rs"));
    }

    #[test]
    fn add_helpers_accumulate_optional_values() {
        let mut target = None;
        add_u64(&mut target, None);
        assert_eq!(target, None);
        add_u64(&mut target, Some(3));
        add_u64(&mut target, Some(4));
        assert_eq!(target, Some(7));

        let mut cost = None;
        add_f64(&mut cost, Some(0.5));
        add_f64(&mut cost, Some(0.25));
        assert_eq!(cost, Some(0.75));

        assert_eq!(min_u128(None, None), None);
        assert_eq!(min_u128(Some(5), None), Some(5));
        assert_eq!(min_u128(None, Some(9)), Some(9));
        assert_eq!(min_u128(Some(5), Some(9)), Some(5));
    }

    #[test]
    fn csv_escape_quotes_special_characters() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn preview_bytes_truncates_long_output() {
        assert_eq!(preview_bytes(b"short"), "short");
        let long = "x".repeat(3000);
        let preview = preview_bytes(long.as_bytes());
        assert!(preview.ends_with("[truncated]"));
        assert!(preview.chars().count() < 2100);
    }

    #[test]
    fn llm_aggregates_sum_tokens_and_keep_min_ttft() {
        let root = temp_root();
        let calls = root.join(".agentledger").join("llm_calls.ndjson");
        let lines = [
            serde_json::json!({
                "run_id": "run-1",
                "duration_ms": 1000,
                "metrics": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10, "cost_usd": 0.001, "ttft_ms": 200}
            }),
            serde_json::json!({
                "run_id": "run-1",
                "duration_ms": 1000,
                "metrics": {"input_tokens": 5, "output_tokens": 5, "total_tokens": 10, "cost_usd": 0.002, "ttft_ms": 120}
            }),
            serde_json::json!({
                "run_id": null,
                "duration_ms": 50,
                "metrics": {"output_tokens": 99}
            }),
        ];
        fs::write(
            &calls,
            lines
                .iter()
                .map(|line| line.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .expect("write llm calls");

        let aggregates = load_llm_metric_aggregates(&root).expect("aggregate");
        assert_eq!(aggregates.len(), 1);
        let aggregate = &aggregates["run-1"];
        assert_eq!(aggregate.call_count, 2);
        assert_eq!(aggregate.metrics.token_input, Some(12));
        assert_eq!(aggregate.metrics.token_output, Some(8));
        assert_eq!(aggregate.metrics.token_total, Some(20));
        assert_eq!(aggregate.metrics.cost_usd, Some(0.003));
        assert_eq!(aggregate.metrics.ttft_ms, Some(120));
        assert_eq!(aggregate.metrics.output_tokens_per_second, Some(4.0));

        let _ = fs::remove_dir_all(&root);
    }

    fn sample_run(id: &str, repo: &Path) -> RunRecord {
        RunRecord {
            id: id.to_string(),
            task: "t".to_string(),
            agent: "a".to_string(),
            command: vec!["true".to_string()],
            repo: repo.to_string_lossy().to_string(),
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

    #[test]
    fn post_hoc_eval_appends_event_and_recomputes_status() {
        let root = temp_root();
        let ledger_dir = root.join(".agentledger");
        append_run_event(&ledger_dir, &sample_run("run-1", &root)).expect("append run");

        let updated = run_post_eval("run-1", &["exit 0".to_string()], &root).expect("passing eval");
        assert_eq!(updated.evals.len(), 1);
        assert_eq!(updated.status, "passed");

        let failed = run_post_eval("run-1", &["exit 1".to_string()], &root).expect("failing eval");
        assert_eq!(failed.evals.len(), 2);
        assert_eq!(failed.status, "failed");

        // Three events in the file, but the latest version of the run wins.
        let runs = load_runs(&root).expect("load runs");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].evals.len(), 2);
        assert_eq!(runs[0].status, "failed");

        assert!(matches!(
            run_post_eval("missing", &["exit 0".to_string()], &root),
            Err(Error::Storage(_))
        ));
        assert!(matches!(
            run_post_eval("run-1", &[], &root),
            Err(Error::Config(_))
        ));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn bench_matrix_parses_with_defaults() {
        let matrix = parse_bench_matrix(
            r#"
[[tasks]]
name = "smoke"
prompt = "say ok"
evals = ["true"]

[[agents]]
name = "custom"
command = ["sh", "-c", "echo {prompt} for {task}"]

[[providers]]
name = "ollama"
upstream = "http://127.0.0.1:11434/v1"
"#,
        )
        .expect("parse matrix");

        assert_eq!(matrix.repeats, 1);
        assert!(!matrix.allow_dirty);
        assert_eq!(matrix.providers[0].bind, "127.0.0.1:0");
        assert!(!matrix.providers[0].record_bodies);

        let command = bench_command(&matrix.agents[0].command, &matrix.tasks[0]);
        assert_eq!(command[2], "echo say ok for smoke");
    }

    #[test]
    fn bench_matrix_rejects_incomplete_definitions() {
        let missing_agents = parse_bench_matrix(
            r#"
[[tasks]]
name = "smoke"
"#,
        );
        assert!(matches!(missing_agents, Err(Error::Config(_))));

        let empty_command = parse_bench_matrix(
            r#"
[[tasks]]
name = "smoke"

[[agents]]
name = "custom"
command = []
"#,
        );
        assert!(matches!(empty_command, Err(Error::Config(_))));
    }

    #[test]
    fn merge_run_metrics_prefers_aggregate_values() {
        let base = RunMetrics {
            llm_metrics_precision: "unknown".to_string(),
            ..RunMetrics::default()
        };
        assert_eq!(
            merge_run_metrics(&base, None).llm_metrics_precision,
            "unknown"
        );

        let aggregate = LlmMetricAggregate {
            call_count: 2,
            duration_ms: 1000,
            metrics: RunMetrics {
                llm_metrics_precision: "exact".to_string(),
                token_input: Some(12),
                token_output: Some(8),
                token_total: Some(20),
                cost_usd: Some(0.003),
                ttft_ms: Some(120),
                output_tokens_per_second: Some(8.0),
                ..RunMetrics::default()
            },
        };
        let merged = merge_run_metrics(&base, Some(&aggregate));
        assert_eq!(merged.llm_metrics_precision, "exact");
        assert_eq!(merged.token_total, Some(20));
        assert_eq!(merged.ttft_ms, Some(120));
        assert_eq!(merged.output_tokens_per_second, Some(8.0));
    }
}
