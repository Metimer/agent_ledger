use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Instant;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Clone)]
struct ProxyState {
    upstream_base: String,
    ledger_root: PathBuf,
    client: reqwest::Client,
    api_key: Option<String>,
    record_bodies: bool,
    default_run_id: Option<String>,
}

pub struct ProxyHandle {
    base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl ProxyHandle {
    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LlmCallRecord {
    record_type: &'static str,
    schema_version: u32,
    id: String,
    timestamp: String,
    endpoint: String,
    run_id: Option<String>,
    upstream_base: String,
    model: Option<String>,
    status: u16,
    duration_ms: u128,
    source_precision: &'static str,
    request_stream: Option<bool>,
    request_body: Option<Value>,
    response_body: Option<Value>,
    metrics: LlmMetrics,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct LlmMetrics {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    cached_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    cost_usd: Option<f64>,
    ttft_ms: Option<u128>,
    output_tokens_per_second: Option<f64>,
}

pub fn serve_proxy(
    bind: &str,
    upstream_base: &str,
    root: &Path,
    api_key: Option<String>,
    record_bodies: bool,
) -> Result<(), String> {
    let runtime = tokio::runtime::Runtime::new().map_err(|err| err.to_string())?;
    runtime.block_on(serve_proxy_async(
        bind.to_string(),
        upstream_base.trim_end_matches('/').to_string(),
        root.to_path_buf(),
        api_key,
        record_bodies,
        None,
    ))
}

pub fn start_proxy_background(
    bind: &str,
    upstream_base: &str,
    root: &Path,
    api_key: Option<String>,
    record_bodies: bool,
    default_run_id: Option<String>,
) -> Result<ProxyHandle, String> {
    ensure_proxy_store(root).map_err(|err| err.to_string())?;
    let std_listener = StdTcpListener::bind(bind).map_err(|err| err.to_string())?;
    std_listener
        .set_nonblocking(true)
        .map_err(|err| err.to_string())?;
    let address = std_listener.local_addr().map_err(|err| err.to_string())?;
    let base_url = format!("http://{address}/v1");
    let upstream_base = upstream_base.trim_end_matches('/').to_string();
    let root = root.to_path_buf();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let thread = thread::spawn(move || {
        let runtime = match tokio::runtime::Runtime::new() {
            Ok(runtime) => runtime,
            Err(err) => {
                eprintln!("agentledger proxy runtime error: {err}");
                return;
            }
        };
        if let Err(err) = runtime.block_on(async move {
            let listener =
                tokio::net::TcpListener::from_std(std_listener).map_err(|err| err.to_string())?;
            run_proxy_server(
                listener,
                upstream_base,
                root,
                api_key,
                record_bodies,
                default_run_id,
                Some(shutdown_rx),
            )
            .await
        }) {
            eprintln!("agentledger proxy server error: {err}");
        }
    });

    Ok(ProxyHandle {
        base_url,
        shutdown: Some(shutdown_tx),
        thread: Some(thread),
    })
}

async fn serve_proxy_async(
    bind: String,
    upstream_base: String,
    root: PathBuf,
    api_key: Option<String>,
    record_bodies: bool,
    default_run_id: Option<String>,
) -> Result<(), String> {
    ensure_proxy_store(&root).map_err(|err| err.to_string())?;
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|err| err.to_string())?;
    run_proxy_server(
        listener,
        upstream_base,
        root,
        api_key,
        record_bodies,
        default_run_id,
        None,
    )
    .await
}

async fn run_proxy_server(
    listener: tokio::net::TcpListener,
    upstream_base: String,
    root: PathBuf,
    api_key: Option<String>,
    record_bodies: bool,
    default_run_id: Option<String>,
    shutdown: Option<oneshot::Receiver<()>>,
) -> Result<(), String> {
    let state = build_state(upstream_base, root, api_key, record_bodies, default_run_id)?;
    let app = proxy_router(state.clone());
    let address = listener.local_addr().map_err(|err| err.to_string())?;
    println!("AgentLedger OpenAI-compatible proxy: http://{address}/v1");
    println!("Forwarding upstream: {}", state.upstream_base);

    let server = axum::serve(listener, app);
    if let Some(shutdown) = shutdown {
        server
            .with_graceful_shutdown(async {
                let _ = shutdown.await;
            })
            .await
            .map_err(|err| err.to_string())
    } else {
        server.await.map_err(|err| err.to_string())
    }
}

fn build_state(
    upstream_base: String,
    root: PathBuf,
    api_key: Option<String>,
    record_bodies: bool,
    default_run_id: Option<String>,
) -> Result<Arc<ProxyState>, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .map_err(|err| err.to_string())?;

    Ok(Arc::new(ProxyState {
        upstream_base,
        ledger_root: root,
        client,
        api_key,
        record_bodies,
        default_run_id,
    }))
}

fn proxy_router(state: Arc<ProxyState>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/chat/completions", post(forward_chat_completions))
        .route("/v1/responses", post(forward_responses))
        .with_state(state)
}

async fn forward_chat_completions(
    State(state): State<Arc<ProxyState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward_openai_endpoint(state, headers, body, "/v1/chat/completions").await
}

async fn forward_responses(
    State(state): State<Arc<ProxyState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    forward_openai_endpoint(state, headers, body, "/v1/responses").await
}

async fn forward_openai_endpoint(
    state: Arc<ProxyState>,
    headers: HeaderMap,
    body: Bytes,
    endpoint: &'static str,
) -> Response {
    let started = Instant::now();
    let request_json = serde_json::from_slice::<Value>(&body).ok();
    let url = upstream_url(&state.upstream_base, endpoint);
    let mut request = state.client.post(url).body(body.clone());

    request = apply_forward_headers(request, &headers, state.api_key.as_deref());

    let response = match request.send().await {
        Ok(response) => response,
        Err(err) => {
            let body = format!("upstream request failed: {err}");
            return (StatusCode::BAD_GATEWAY, body).into_response();
        }
    };

    let status = response.status();
    let response_headers = response.headers().clone();
    let response_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => {
            let body = format!("upstream response read failed: {err}");
            return (StatusCode::BAD_GATEWAY, body).into_response();
        }
    };
    let duration_ms = started.elapsed().as_millis();
    let response_json = serde_json::from_slice::<Value>(&response_bytes).ok();

    let record = build_record(
        endpoint,
        extract_run_id(&headers).or_else(|| state.default_run_id.clone()),
        &state.upstream_base,
        status.as_u16(),
        duration_ms,
        request_json,
        response_json,
        state.record_bodies,
    );
    if let Err(err) = append_llm_call(&state.ledger_root, &record) {
        eprintln!("agentledger proxy record error: {err}");
    }

    response_from_upstream(status, &response_headers, response_bytes)
}

fn upstream_url(upstream_base: &str, endpoint: &str) -> String {
    if upstream_base.ends_with("/v1") && endpoint.starts_with("/v1/") {
        format!("{}{}", upstream_base, &endpoint[3..])
    } else {
        format!("{upstream_base}{endpoint}")
    }
}

fn extract_run_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-agentledger-run-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn apply_forward_headers(
    mut request: reqwest::RequestBuilder,
    headers: &HeaderMap,
    api_key: Option<&str>,
) -> reqwest::RequestBuilder {
    for name in [
        "accept",
        "content-type",
        "openai-organization",
        "openai-project",
        "anthropic-beta",
        "x-title",
        "http-referer",
    ] {
        if let (Ok(header_name), Some(value)) =
            (HeaderName::from_bytes(name.as_bytes()), headers.get(name))
        {
            request = request.header(header_name.as_str(), value.as_bytes());
        }
    }

    if let Some(api_key) = api_key {
        request = request.bearer_auth(api_key);
    } else if let Some(value) = headers.get("authorization") {
        request = request.header("authorization", value.as_bytes());
    }

    request
}

fn response_from_upstream(
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
    body: Bytes,
) -> Response {
    let mut response = (
        StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
        body,
    )
        .into_response();
    for name in ["content-type", "cache-control", "content-length"] {
        if let Some(value) = headers.get(name) {
            if let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) {
                response.headers_mut().insert(name, value);
            }
        }
    }
    response
}

#[allow(clippy::too_many_arguments)]
fn build_record(
    endpoint: &str,
    run_id: Option<String>,
    upstream_base: &str,
    status: u16,
    duration_ms: u128,
    request_json: Option<Value>,
    response_json: Option<Value>,
    record_bodies: bool,
) -> LlmCallRecord {
    let model = request_json
        .as_ref()
        .and_then(|body| body.get("model"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            response_json
                .as_ref()
                .and_then(|body| body.get("model"))
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    let request_stream = request_json
        .as_ref()
        .and_then(|body| body.get("stream"))
        .and_then(Value::as_bool);
    let metrics = extract_metrics(response_json.as_ref(), duration_ms);

    LlmCallRecord {
        record_type: "llm_call",
        schema_version: 1,
        id: Uuid::new_v4().to_string(),
        timestamp: now_rfc3339(),
        endpoint: endpoint.to_string(),
        run_id,
        upstream_base: upstream_base.to_string(),
        model,
        status,
        duration_ms,
        source_precision: "exact",
        request_stream,
        request_body: record_bodies.then(|| request_json.clone()).flatten(),
        response_body: record_bodies.then(|| response_json.clone()).flatten(),
        metrics,
    }
}

fn extract_metrics(response_json: Option<&Value>, duration_ms: u128) -> LlmMetrics {
    let usage = response_json.and_then(|body| body.get("usage"));
    let input_tokens = first_u64(
        usage,
        &["prompt_tokens", "input_tokens", "prompt_eval_count"],
    );
    let output_tokens = first_u64(usage, &["completion_tokens", "output_tokens", "eval_count"]);
    let total_tokens = first_u64(usage, &["total_tokens"]).or_else(|| {
        input_tokens
            .zip(output_tokens)
            .map(|(input, output)| input + output)
    });
    let cached_tokens = usage
        .and_then(|usage| usage.get("prompt_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64);
    let reasoning_tokens = usage
        .and_then(|usage| usage.get("completion_tokens_details"))
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_u64);
    let cost_usd = response_json
        .and_then(|body| body.get("usage"))
        .and_then(|usage| usage.get("cost"))
        .and_then(Value::as_f64)
        .or_else(|| {
            response_json
                .and_then(|body| body.get("cost"))
                .and_then(Value::as_f64)
        });
    let output_tokens_per_second = output_tokens.and_then(|tokens| {
        if duration_ms == 0 {
            None
        } else {
            Some(tokens as f64 / (duration_ms as f64 / 1000.0))
        }
    });

    LlmMetrics {
        input_tokens,
        output_tokens,
        total_tokens,
        cached_tokens,
        reasoning_tokens,
        cost_usd,
        ttft_ms: None,
        output_tokens_per_second,
    }
}

fn first_u64(root: Option<&Value>, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        root.and_then(|value| value.get(*key))
            .and_then(Value::as_u64)
    })
}

fn ensure_proxy_store(root: &Path) -> std::io::Result<()> {
    let ledger_dir = root.join(".agentledger");
    fs::create_dir_all(&ledger_dir)?;
    let calls = ledger_dir.join("llm_calls.ndjson");
    if !calls.exists() {
        std::fs::File::create(calls)?;
    }
    Ok(())
}

fn append_llm_call(root: &Path, record: &LlmCallRecord) -> std::io::Result<()> {
    ensure_proxy_store(root)?;
    let calls = root.join(".agentledger").join("llm_calls.ndjson");
    let mut file = OpenOptions::new().create(true).append(true).open(calls)?;
    let line = serde_json::to_string(record)?;
    writeln!(file, "{line}")?;
    Ok(())
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}
