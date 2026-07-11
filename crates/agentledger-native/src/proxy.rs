use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
    // Calls that failed (HTTP >= 400 or unreachable upstream), shared with ProxyHandle.
    error_calls: Arc<AtomicU64>,
}

pub struct ProxyHandle {
    base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    error_calls: Arc<AtomicU64>,
}

impl ProxyHandle {
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn error_calls_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.error_calls)
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
    let error_calls = Arc::new(AtomicU64::new(0));
    let error_calls_worker = Arc::clone(&error_calls);

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
                error_calls_worker,
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
        error_calls,
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
        Arc::new(AtomicU64::new(0)),
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_proxy_server(
    listener: tokio::net::TcpListener,
    upstream_base: String,
    root: PathBuf,
    api_key: Option<String>,
    record_bodies: bool,
    default_run_id: Option<String>,
    error_calls: Arc<AtomicU64>,
    shutdown: Option<oneshot::Receiver<()>>,
) -> Result<(), String> {
    let state = build_state(
        upstream_base,
        root,
        api_key,
        record_bodies,
        default_run_id,
        error_calls,
    )?;
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
    error_calls: Arc<AtomicU64>,
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
        error_calls,
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
    let run_id = extract_run_id(&headers).or_else(|| state.default_run_id.clone());
    let url = upstream_url(&state.upstream_base, endpoint);
    let mut request = state.client.post(url).body(body.clone());

    request = apply_forward_headers(request, &headers, state.api_key.as_deref());

    let response = match request.send().await {
        Ok(response) => response,
        Err(err) => {
            state.error_calls.fetch_add(1, Ordering::SeqCst);
            let body = format!("upstream request failed: {err}");
            return (StatusCode::BAD_GATEWAY, body).into_response();
        }
    };

    let status = response.status();
    if status.as_u16() >= 400 {
        state.error_calls.fetch_add(1, Ordering::SeqCst);
    }
    let response_headers = response.headers().clone();

    if is_event_stream(&response_headers) {
        return stream_openai_response(
            state,
            endpoint,
            run_id,
            request_json,
            started,
            status,
            response_headers,
            response,
        );
    }

    let response_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => {
            if status.as_u16() < 400 {
                state.error_calls.fetch_add(1, Ordering::SeqCst);
            }
            let body = format!("upstream response read failed: {err}");
            return (StatusCode::BAD_GATEWAY, body).into_response();
        }
    };
    let duration_ms = started.elapsed().as_millis();
    let response_json = serde_json::from_slice::<Value>(&response_bytes).ok();

    let record = build_record(
        endpoint,
        run_id,
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

fn is_event_stream(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("text/event-stream")
        })
        .unwrap_or(false)
}

#[allow(clippy::too_many_arguments)]
fn stream_openai_response(
    state: Arc<ProxyState>,
    endpoint: &'static str,
    run_id: Option<String>,
    request_json: Option<Value>,
    started: Instant,
    status: reqwest::StatusCode,
    response_headers: reqwest::header::HeaderMap,
    response: reqwest::Response,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<Bytes, std::io::Error>>(32);
    let record_bodies = state.record_bodies;
    let upstream_base = state.upstream_base.clone();
    let ledger_root = state.ledger_root.clone();
    let error_calls = Arc::clone(&state.error_calls);
    let status_code = status.as_u16();

    tokio::spawn(async move {
        let mut collector = SseCollector::new(started, record_bodies);
        let mut upstream = response.bytes_stream();
        let mut client_connected = true;
        while let Some(item) = upstream.next().await {
            match item {
                Ok(bytes) => {
                    collector.observe(&bytes);
                    if client_connected && tx.send(Ok(bytes)).await.is_err() {
                        client_connected = false;
                    }
                }
                Err(err) => {
                    if status_code < 400 {
                        error_calls.fetch_add(1, Ordering::SeqCst);
                    }
                    if client_connected {
                        let _ = tx.send(Err(std::io::Error::other(err.to_string()))).await;
                    }
                    break;
                }
            }
        }
        let duration_ms = started.elapsed().as_millis();
        let record = collector.finish(
            endpoint,
            run_id,
            &upstream_base,
            status_code,
            request_json.as_ref(),
            duration_ms,
        );
        if let Err(err) = append_llm_call(&ledger_root, &record) {
            eprintln!("agentledger proxy record error: {err}");
        }
    });

    let body_stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    let mut response = Response::new(Body::from_stream(body_stream));
    *response.status_mut() = StatusCode::from_u16(status_code).unwrap_or(StatusCode::BAD_GATEWAY);
    for name in ["content-type", "cache-control"] {
        if let Some(value) = response_headers.get(name) {
            if let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) {
                response.headers_mut().insert(name, value);
            }
        }
    }
    response
}

struct SseCollector {
    started: Instant,
    record_bodies: bool,
    line_buffer: Vec<u8>,
    ttft_ms: Option<u128>,
    last_output_ms: Option<u128>,
    delta_count: u64,
    content: String,
    model: Option<String>,
    usage: Option<Value>,
}

impl SseCollector {
    fn new(started: Instant, record_bodies: bool) -> Self {
        Self {
            started,
            record_bodies,
            line_buffer: Vec::new(),
            ttft_ms: None,
            last_output_ms: None,
            delta_count: 0,
            content: String::new(),
            model: None,
            usage: None,
        }
    }

    fn observe(&mut self, chunk: &[u8]) {
        self.line_buffer.extend_from_slice(chunk);
        while let Some(newline) = self.line_buffer.iter().position(|&byte| byte == b'\n') {
            let line = self.line_buffer.drain(..=newline).collect::<Vec<_>>();
            let line = String::from_utf8_lossy(&line);
            self.observe_line(&line);
        }
    }

    fn observe_line(&mut self, line: &str) {
        let line = line.trim_end_matches(['\r', '\n']);
        let Some(data) = line.strip_prefix("data:") else {
            return;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            return;
        }
        if let Ok(event) = serde_json::from_str::<Value>(data) {
            self.observe_event(&event);
        }
    }

    fn observe_event(&mut self, event: &Value) {
        if self.model.is_none() {
            self.model = event
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    event
                        .pointer("/response/model")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                });
        }
        if let Some(usage) = event
            .get("usage")
            .filter(|usage| usage.is_object())
            .or_else(|| {
                event
                    .pointer("/response/usage")
                    .filter(|usage| usage.is_object())
            })
        {
            self.usage = Some(usage.clone());
        }

        let delta_text = chat_delta_text(event).or_else(|| responses_delta_text(event));
        let has_output = delta_text.is_some() || chat_delta_has_tool_output(event);
        if !has_output {
            return;
        }

        let elapsed = self.started.elapsed().as_millis();
        if self.ttft_ms.is_none() {
            self.ttft_ms = Some(elapsed);
        }
        self.last_output_ms = Some(elapsed);
        self.delta_count += 1;
        if self.record_bodies {
            if let Some(text) = delta_text {
                self.content.push_str(&text);
            }
        }
    }

    fn finish(
        self,
        endpoint: &str,
        run_id: Option<String>,
        upstream_base: &str,
        status: u16,
        request_json: Option<&Value>,
        duration_ms: u128,
    ) -> LlmCallRecord {
        let synthetic_response = self
            .usage
            .as_ref()
            .map(|usage| serde_json::json!({ "usage": usage }));
        let mut metrics = extract_metrics(synthetic_response.as_ref(), duration_ms);
        let source_precision = if self.usage.is_some() {
            "exact"
        } else {
            "estimated"
        };
        if metrics.output_tokens.is_none() && self.delta_count > 0 {
            metrics.output_tokens = Some(self.delta_count);
        }
        metrics.ttft_ms = self.ttft_ms;
        if let Some(output_tokens) = metrics.output_tokens {
            let generation_ms = match (self.ttft_ms, self.last_output_ms) {
                (Some(first), Some(last)) if last > first => last - first,
                _ => duration_ms,
            };
            if generation_ms > 0 {
                metrics.output_tokens_per_second =
                    Some(output_tokens as f64 / (generation_ms as f64 / 1000.0));
            }
        }

        let model = request_json
            .and_then(|body| body.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(self.model.clone());
        let request_stream = request_json
            .and_then(|body| body.get("stream"))
            .and_then(Value::as_bool)
            .or(Some(true));
        let response_body = self.record_bodies.then(|| {
            serde_json::json!({
                "stream": true,
                "model": self.model,
                "content": self.content,
                "usage": self.usage,
            })
        });

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
            source_precision,
            request_stream,
            request_body: self.record_bodies.then(|| request_json.cloned()).flatten(),
            response_body,
            metrics,
        }
    }
}

fn chat_delta_text(event: &Value) -> Option<String> {
    let choices = event.get("choices")?.as_array()?;
    let mut text = String::new();
    for choice in choices {
        if let Some(content) = choice.pointer("/delta/content").and_then(Value::as_str) {
            text.push_str(content);
        }
    }
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn chat_delta_has_tool_output(event: &Value) -> bool {
    event
        .get("choices")
        .and_then(Value::as_array)
        .map(|choices| {
            choices.iter().any(|choice| {
                choice
                    .pointer("/delta/tool_calls")
                    .is_some_and(|value| !value.is_null())
                    || choice
                        .pointer("/delta/function_call")
                        .is_some_and(|value| !value.is_null())
                    || choice
                        .pointer("/delta/reasoning_content")
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.is_empty())
            })
        })
        .unwrap_or(false)
}

fn responses_delta_text(event: &Value) -> Option<String> {
    let event_type = event.get("type")?.as_str()?;
    if !event_type.ends_with(".delta") {
        return None;
    }
    event
        .get("delta")
        .and_then(Value::as_str)
        .map(str::to_string)
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header;

    fn collector() -> SseCollector {
        SseCollector::new(Instant::now(), true)
    }

    #[test]
    fn upstream_url_avoids_duplicate_v1() {
        assert_eq!(
            upstream_url("http://localhost:11434/v1", "/v1/chat/completions"),
            "http://localhost:11434/v1/chat/completions"
        );
        assert_eq!(
            upstream_url("http://localhost:8080", "/v1/chat/completions"),
            "http://localhost:8080/v1/chat/completions"
        );
    }

    #[test]
    fn extract_run_id_reads_trimmed_header() {
        let mut headers = HeaderMap::new();
        assert_eq!(extract_run_id(&headers), None);
        headers.insert("x-agentledger-run-id", "  run-42  ".parse().unwrap());
        assert_eq!(extract_run_id(&headers), Some("run-42".to_string()));
        headers.insert("x-agentledger-run-id", "   ".parse().unwrap());
        assert_eq!(extract_run_id(&headers), None);
    }

    #[test]
    fn extract_metrics_reads_openai_usage() {
        let response = serde_json::json!({
            "usage": {
                "prompt_tokens": 11,
                "completion_tokens": 5,
                "total_tokens": 16,
                "prompt_tokens_details": { "cached_tokens": 4 },
                "completion_tokens_details": { "reasoning_tokens": 2 },
                "cost": 0.001,
            }
        });
        let metrics = extract_metrics(Some(&response), 2000);
        assert_eq!(metrics.input_tokens, Some(11));
        assert_eq!(metrics.output_tokens, Some(5));
        assert_eq!(metrics.total_tokens, Some(16));
        assert_eq!(metrics.cached_tokens, Some(4));
        assert_eq!(metrics.reasoning_tokens, Some(2));
        assert_eq!(metrics.cost_usd, Some(0.001));
        assert_eq!(metrics.output_tokens_per_second, Some(2.5));
    }

    #[test]
    fn extract_metrics_reads_ollama_usage_and_sums_total() {
        let response = serde_json::json!({
            "usage": { "prompt_eval_count": 7, "eval_count": 3 }
        });
        let metrics = extract_metrics(Some(&response), 1000);
        assert_eq!(metrics.input_tokens, Some(7));
        assert_eq!(metrics.output_tokens, Some(3));
        assert_eq!(metrics.total_tokens, Some(10));
    }

    #[test]
    fn sse_collector_parses_chat_stream_split_across_chunks() {
        let mut collector = collector();
        let first = "data: {\"model\":\"mock-model\",\"choices\":[{\"delta\":{\"content\":\"He\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"con";
        let second = "tent\":\"llo\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5,\"total_tokens\":16}}\n\ndata: [DONE]\n\n";
        collector.observe(first.as_bytes());
        collector.observe(second.as_bytes());

        assert_eq!(collector.delta_count, 2);
        assert_eq!(collector.content, "Hello");
        assert_eq!(collector.model.as_deref(), Some("mock-model"));
        assert!(collector.ttft_ms.is_some());
        let usage = collector.usage.as_ref().expect("usage captured");
        assert_eq!(usage.get("total_tokens").and_then(Value::as_u64), Some(16));
    }

    #[test]
    fn sse_collector_parses_responses_api_events() {
        let mut collector = collector();
        let stream = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"model\":\"mock-model\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":8,\"output_tokens\":2,\"total_tokens\":10}}}\n\n",
        );
        collector.observe(stream.as_bytes());

        assert_eq!(collector.delta_count, 1);
        assert_eq!(collector.content, "Hi");
        assert_eq!(collector.model.as_deref(), Some("mock-model"));
        let usage = collector.usage.as_ref().expect("usage captured");
        assert_eq!(usage.get("output_tokens").and_then(Value::as_u64), Some(2));
    }

    #[test]
    fn sse_collector_finish_uses_usage_when_present() {
        let mut collector = collector();
        collector.observe(
            concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5,\"total_tokens\":16}}\n\n",
                "data: [DONE]\n\n",
            )
            .as_bytes(),
        );
        let request = serde_json::json!({"model": "req-model", "stream": true});
        let record = collector.finish(
            "/v1/chat/completions",
            Some("run-1".to_string()),
            "http://upstream/v1",
            200,
            Some(&request),
            1000,
        );

        assert_eq!(record.source_precision, "exact");
        assert_eq!(record.model.as_deref(), Some("req-model"));
        assert_eq!(record.request_stream, Some(true));
        assert_eq!(record.run_id.as_deref(), Some("run-1"));
        assert_eq!(record.metrics.input_tokens, Some(11));
        assert_eq!(record.metrics.output_tokens, Some(5));
        assert!(record.metrics.ttft_ms.is_some());
        assert!(record.metrics.output_tokens_per_second.is_some());
        let body = record.response_body.as_ref().expect("body recorded");
        assert_eq!(body.get("content").and_then(Value::as_str), Some("ok"));
    }

    #[test]
    fn sse_collector_finish_estimates_tokens_without_usage() {
        let mut collector = SseCollector::new(Instant::now(), false);
        collector.observe(
            concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n",
                "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n",
                "data: {\"choices\":[{\"delta\":{\"content\":\"c\"}}]}\n\n",
                "data: [DONE]\n\n",
            )
            .as_bytes(),
        );
        let record = collector.finish("/v1/chat/completions", None, "http://up/v1", 200, None, 500);

        assert_eq!(record.source_precision, "estimated");
        assert_eq!(record.metrics.output_tokens, Some(3));
        assert_eq!(record.metrics.input_tokens, None);
        assert!(record.response_body.is_none());
    }

    #[test]
    fn sse_collector_counts_tool_call_deltas() {
        let mut collector = collector();
        collector.observe(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"f\"}}]}}]}\n\n"
                .as_bytes(),
        );
        assert_eq!(collector.delta_count, 1);
        assert!(collector.ttft_ms.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn proxy_counts_upstream_error_calls() {
        let upstream_app = Router::new().route(
            "/v1/chat/completions",
            post(|| async {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    "{\"error\":\"rate limited\"}",
                )
            }),
        );
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upstream");
        let upstream_address = upstream_listener.local_addr().expect("upstream addr");
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app)
                .await
                .expect("upstream serve");
        });

        let root = std::env::temp_dir().join(format!("agentledger-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create test root");
        let proxy = start_proxy_background(
            "127.0.0.1:0",
            &format!("http://{upstream_address}/v1"),
            &root,
            None,
            false,
            None,
        )
        .expect("start proxy");
        let errors = proxy.error_calls_handle();

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/chat/completions", proxy.base_url()))
            .json(&serde_json::json!({"model": "mock"}))
            .send()
            .await
            .expect("proxy request");
        assert_eq!(response.status().as_u16(), 429);

        drop(proxy);
        assert_eq!(errors.load(Ordering::SeqCst), 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn proxy_streams_sse_and_records_metrics() {
        let sse_body = concat!(
            "data: {\"model\":\"mock-model\",\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5,\"total_tokens\":16}}\n\n",
            "data: [DONE]\n\n",
        );
        let upstream_app = Router::new().route(
            "/v1/chat/completions",
            post(move || async move { ([(header::CONTENT_TYPE, "text/event-stream")], sse_body) }),
        );
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind upstream");
        let upstream_address = upstream_listener.local_addr().expect("upstream addr");
        tokio::spawn(async move {
            axum::serve(upstream_listener, upstream_app)
                .await
                .expect("upstream serve");
        });

        let root = std::env::temp_dir().join(format!("agentledger-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create test root");
        let proxy = start_proxy_background(
            "127.0.0.1:0",
            &format!("http://{upstream_address}/v1"),
            &root,
            None,
            false,
            Some("run-stream".to_string()),
        )
        .expect("start proxy");

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/chat/completions", proxy.base_url()))
            .json(&serde_json::json!({"model": "mock-model", "stream": true}))
            .send()
            .await
            .expect("proxy request");
        assert_eq!(response.status().as_u16(), 200);
        assert!(is_event_stream(response.headers()));
        let body = response.text().await.expect("proxy body");
        assert_eq!(body, sse_body);

        let calls_path = root.join(".agentledger").join("llm_calls.ndjson");
        let mut line = String::new();
        for _ in 0..200 {
            line = fs::read_to_string(&calls_path).unwrap_or_default();
            if !line.trim().is_empty() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(!line.trim().is_empty(), "llm call record was not written");
        let record: Value =
            serde_json::from_str(line.lines().next().unwrap()).expect("record json");
        assert_eq!(
            record.get("run_id").and_then(Value::as_str),
            Some("run-stream")
        );
        assert_eq!(
            record.get("source_precision").and_then(Value::as_str),
            Some("exact")
        );
        assert_eq!(
            record
                .pointer("/metrics/total_tokens")
                .and_then(Value::as_u64),
            Some(16)
        );
        assert!(record
            .pointer("/metrics/ttft_ms")
            .and_then(Value::as_u64)
            .is_some());

        drop(proxy);
        let _ = fs::remove_dir_all(&root);
    }
}
