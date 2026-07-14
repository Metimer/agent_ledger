use crate::db;
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

const DASHBOARD_HTML: &str = include_str!("../assets/dashboard.html");

struct DashboardState {
    root: PathBuf,
    token: String,
}

pub fn serve_dashboard(bind: &str, root: &Path) -> Result<(), String> {
    let runtime = tokio::runtime::Runtime::new().map_err(|err| err.to_string())?;
    let root = root.to_path_buf();
    let bind = bind.to_string();
    runtime.block_on(async move {
        db::sync(&root)?;
        let listener = tokio::net::TcpListener::bind(&bind)
            .await
            .map_err(|err| err.to_string())?;
        let address = listener.local_addr().map_err(|err| err.to_string())?;
        let token = Uuid::new_v4().to_string();
        println!("AgentLedger dashboard: http://{address}/?token={token}");

        let app = build_router(root, token);
        axum::serve(listener, app)
            .await
            .map_err(|err| err.to_string())
    })
}

fn build_router(root: PathBuf, token: String) -> Router {
    let state = Arc::new(DashboardState { root, token });
    Router::new()
        .route("/", get(index))
        .route("/api/runs", get(api_runs))
        .route("/api/runs/{id}", get(api_run_detail))
        .route("/api/runs/{id}/output", get(api_run_output))
        .route("/api/tasks", get(api_tasks))
        .route("/api/models", get(api_models))
        .route("/api/prompts", get(api_prompts))
        .route("/api/timeseries", get(api_timeseries))
        .with_state(state)
}

fn authorized(
    state: &DashboardState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> bool {
    if params
        .get("token")
        .is_some_and(|token| *token == state.token)
    {
        return true;
    }
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {}", state.token))
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "missing or invalid token").into_response()
}

fn storage_error(err: String) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, err).into_response()
}

fn filter_param<'a>(params: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    params
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

async fn index(
    State(state): State<Arc<DashboardState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers, &params) {
        return unauthorized();
    }
    Html(DASHBOARD_HTML).into_response()
}

async fn api_runs(
    State(state): State<Arc<DashboardState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers, &params) {
        return unauthorized();
    }
    if let Err(err) = db::sync(&state.root) {
        return storage_error(err);
    }
    let filters = db::RunFilters {
        task: filter_param(&params, "task"),
        agent: filter_param(&params, "agent"),
        status: filter_param(&params, "status"),
        since: filter_param(&params, "since"),
        limit: filter_param(&params, "limit")
            .and_then(|value| value.parse().ok())
            .unwrap_or(500),
    };
    match db::list_runs(&state.root, &filters) {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => storage_error(err),
    }
}

async fn api_run_detail(
    State(state): State<Arc<DashboardState>>,
    UrlPath(run_id): UrlPath<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers, &params) {
        return unauthorized();
    }
    if let Err(err) = db::sync(&state.root) {
        return storage_error(err);
    }
    match db::run_detail(&state.root, &run_id) {
        Ok(Some(detail)) => Json(detail).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "run not found").into_response(),
        Err(err) => storage_error(err),
    }
}

async fn api_run_output(
    State(state): State<Arc<DashboardState>>,
    UrlPath(run_id): UrlPath<String>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers, &params) {
        return unauthorized();
    }
    let stream = params.get("stream").map(String::as_str).unwrap_or("stdout");
    match db::run_output_path(&state.root, &run_id, stream) {
        Ok(Some(path)) if !path.is_empty() => match fs::read_to_string(&path) {
            Ok(content) => (
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                content,
            )
                .into_response(),
            Err(err) => (
                StatusCode::NOT_FOUND,
                format!("output file unavailable: {err}"),
            )
                .into_response(),
        },
        Ok(_) => (StatusCode::NOT_FOUND, "run or output not found").into_response(),
        Err(err) => storage_error(err),
    }
}

async fn api_tasks(
    State(state): State<Arc<DashboardState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers, &params) {
        return unauthorized();
    }
    if let Err(err) = db::sync(&state.root) {
        return storage_error(err);
    }
    match db::task_aggregates(&state.root) {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => storage_error(err),
    }
}

async fn api_models(
    State(state): State<Arc<DashboardState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers, &params) {
        return unauthorized();
    }
    if let Err(err) = db::sync(&state.root) {
        return storage_error(err);
    }
    match db::model_aggregates(
        &state.root,
        filter_param(&params, "task"),
        filter_param(&params, "prompt"),
    ) {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => storage_error(err),
    }
}

async fn api_prompts(
    State(state): State<Arc<DashboardState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers, &params) {
        return unauthorized();
    }
    if let Err(err) = db::sync(&state.root) {
        return storage_error(err);
    }
    match db::prompt_options(&state.root, filter_param(&params, "task")) {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => storage_error(err),
    }
}

async fn api_timeseries(
    State(state): State<Arc<DashboardState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers, &params) {
        return unauthorized();
    }
    if let Err(err) = db::sync(&state.root) {
        return storage_error(err);
    }
    match db::timeseries(&state.root, filter_param(&params, "task")) {
        Ok(rows) => Json(rows).into_response(),
        Err(err) => storage_error(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{append_run_event, GitSnapshot, RunMetrics, RunRecord};
    use serde_json::{json, Value};

    fn temp_root_with_run() -> PathBuf {
        let root = std::env::temp_dir().join(format!("agentledger-dash-test-{}", Uuid::new_v4()));
        let ledger_dir = root.join(".agentledger");
        fs::create_dir_all(&ledger_dir).expect("create ledger");
        let run = RunRecord {
            id: "run-dash".to_string(),
            task: "smoke".to_string(),
            agent: "custom".to_string(),
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
        };
        append_run_event(&ledger_dir, &run).expect("append run");
        let call = json!({
            "id": "call-1",
            "run_id": "run-dash",
            "model": "mock-model",
            "prompt": "dis bonjour",
            "upstream_base": "http://127.0.0.1:4141/v1",
            "status": 200,
            "duration_ms": 100,
            "metrics": { "input_tokens": 3, "output_tokens": 5, "total_tokens": 8 }
        });
        fs::write(ledger_dir.join("llm_calls.ndjson"), format!("{call}\n"))
            .expect("write llm call");
        root
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn api_requires_token_and_serves_data() {
        let root = temp_root_with_run();
        let token = "test-token".to_string();
        let app = build_router(root.clone(), token.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let client = reqwest::Client::new();
        let base = format!("http://{address}");

        let unauthorized = client
            .get(format!("{base}/api/runs"))
            .send()
            .await
            .expect("request");
        assert_eq!(unauthorized.status().as_u16(), 401);

        let html = client
            .get(format!("{base}/?token={token}"))
            .send()
            .await
            .expect("index");
        assert_eq!(html.status().as_u16(), 200);
        assert!(html.text().await.expect("body").contains("AgentLedger"));

        let runs: Value = client
            .get(format!("{base}/api/runs?token={token}&task=smoke"))
            .send()
            .await
            .expect("runs")
            .json()
            .await
            .expect("json");
        assert_eq!(runs.as_array().map(Vec::len), Some(1));
        assert_eq!(runs[0]["agent"], json!("custom"));

        let filtered: Value = client
            .get(format!("{base}/api/runs?token={token}&task=absente"))
            .send()
            .await
            .expect("filtered")
            .json()
            .await
            .expect("json");
        assert_eq!(filtered.as_array().map(Vec::len), Some(0));

        let aggregates: Value = client
            .get(format!("{base}/api/tasks?token={token}"))
            .send()
            .await
            .expect("tasks")
            .json()
            .await
            .expect("json");
        assert_eq!(aggregates[0]["task"], json!("smoke"));
        assert_eq!(aggregates[0]["runs"], json!(1));

        let models: Value = client
            .get(format!("{base}/api/models?token={token}"))
            .send()
            .await
            .expect("models")
            .json()
            .await
            .expect("json");
        assert_eq!(models[0]["model"], json!("mock-model"));
        assert_eq!(models[0]["calls"], json!(1));
        assert_eq!(models[0]["token_output"], json!(5));

        let filtered_models: Value = client
            .get(format!(
                "{base}/api/models?token={token}&prompt=autre+prompt"
            ))
            .send()
            .await
            .expect("filtered models")
            .json()
            .await
            .expect("json");
        assert_eq!(filtered_models.as_array().map(Vec::len), Some(0));

        let prompts: Value = client
            .get(format!("{base}/api/prompts?token={token}"))
            .send()
            .await
            .expect("prompts")
            .json()
            .await
            .expect("json");
        assert_eq!(prompts[0]["prompt"], json!("dis bonjour"));

        let prompts_unauthorized = client
            .get(format!("{base}/api/prompts"))
            .send()
            .await
            .expect("prompts unauthorized");
        assert_eq!(prompts_unauthorized.status().as_u16(), 401);

        let detail: Value = client
            .get(format!("{base}/api/runs/run-dash?token={token}"))
            .send()
            .await
            .expect("detail")
            .json()
            .await
            .expect("json");
        assert_eq!(detail["run"]["id"], json!("run-dash"));

        let missing = client
            .get(format!("{base}/api/runs/absent?token={token}"))
            .send()
            .await
            .expect("missing");
        assert_eq!(missing.status().as_u16(), 404);

        let _ = fs::remove_dir_all(&root);
    }
}
