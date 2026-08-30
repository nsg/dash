use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::store::{InputPoint, Store, segment_matches};

const INDEX: &str = include_str!("../assets/index.html");
const UPLOT_JS: &[u8] = include_bytes!("../assets/uPlot.iife.min.js");
const UPLOT_CSS: &[u8] = include_bytes!("../assets/uPlot.min.css");

#[derive(Clone)]
struct AppState {
    store: Arc<Store>,
}

#[derive(Deserialize)]
struct WirePoint {
    m: String,
    v: f64,
    ts: Option<i64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum IngestBody {
    One(WirePoint),
    Many(Vec<WirePoint>),
}

#[derive(Serialize)]
struct QueryResponse {
    step: i64,
    from: i64,
    to: i64,
    truncated: bool,
    series: Vec<QuerySeries>,
}

#[derive(Serialize)]
struct QuerySeries {
    m: String,
    points: Vec<(i64, Option<f64>)>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct FindNode {
    path: String,
    name: String,
    leaf: bool,
    leaves: usize,
}

#[derive(Deserialize)]
struct FindParams {
    q: Option<String>,
}

pub fn router(store: Arc<Store>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/assets/uPlot.iife.min.js", get(uplot_js))
        .route("/assets/uPlot.min.css", get(uplot_css))
        .route("/ingest", post(ingest))
        .route("/series", get(series))
        .route("/find", get(find))
        .route("/query", get(query))
        .route("/healthz", get(healthz))
        .with_state(AppState { store })
}

pub fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch");
    i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
}

async fn index() -> Response {
    static_response(
        header::CONTENT_TYPE,
        "text/html; charset=utf-8",
        INDEX.as_bytes(),
    )
}

async fn uplot_js() -> Response {
    static_response(header::CONTENT_TYPE, "application/javascript", UPLOT_JS)
}

async fn uplot_css() -> Response {
    static_response(header::CONTENT_TYPE, "text/css", UPLOT_CSS)
}

fn static_response(name: header::HeaderName, value: &'static str, body: &'static [u8]) -> Response {
    ([(name, value)], body).into_response()
}

async fn ingest(State(state): State<AppState>, body: Bytes) -> Response {
    let parsed = match serde_json::from_slice::<IngestBody>(&body) {
        Ok(IngestBody::One(point)) => vec![point],
        Ok(IngestBody::Many(points)) => points,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "invalid JSON body"),
    };
    let now = now_ms();
    let points = parsed
        .into_iter()
        .map(|point| InputPoint {
            name: point.m,
            value: point.v,
            timestamp: point.ts.unwrap_or(now),
        })
        .collect();
    Json(state.store.ingest(points)).into_response()
}

async fn series(State(state): State<AppState>) -> Json<Vec<String>> {
    Json(state.store.series_names())
}

async fn find(
    State(state): State<AppState>,
    Query(params): Query<FindParams>,
) -> Json<Vec<FindNode>> {
    let query = params.q.as_deref().unwrap_or("*");
    Json(find_nodes(&state.store.series_names(), query))
}

async fn query(
    State(state): State<AppState>,
    Query(params): Query<Vec<(String, String)>>,
) -> Response {
    let patterns: Vec<_> = params
        .iter()
        .filter(|(key, _)| key == "m")
        .map(|(_, value)| value.as_str())
        .collect();
    if patterns.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "missing metric parameter");
    }

    let now = now_ms();
    let from = match parse_time(parameter(&params, "from").unwrap_or("-1h"), now) {
        Ok(value) => value,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    let to = match parse_time(parameter(&params, "to").unwrap_or("now"), now) {
        Ok(value) => value,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    if from >= to {
        return json_error(StatusCode::BAD_REQUEST, "from must be before to");
    }
    let span = match to.checked_sub(from) {
        Some(span) => span,
        None => return json_error(StatusCode::BAD_REQUEST, "time range is out of range"),
    };
    let step = match parameter(&params, "step") {
        Some(value) => match parse_step(value) {
            Ok(value) => value,
            Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
        },
        None => (span / 300).max(1_000),
    };

    let mut metrics = BTreeSet::new();
    for pattern in patterns {
        metrics.extend(state.store.expand(pattern));
    }
    let truncated = metrics.len() > 20;
    let series = metrics
        .into_iter()
        .take(20)
        .filter_map(|metric| {
            state
                .store
                .query(&metric, from, to, step)
                .map(|points| QuerySeries { m: metric, points })
        })
        .collect();

    Json(QueryResponse {
        step,
        from,
        to,
        truncated,
        series,
    })
    .into_response()
}

async fn healthz(State(state): State<AppState>) -> Response {
    Json(state.store.stats()).into_response()
}

fn json_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

fn parameter<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    params
        .iter()
        .rev()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn find_nodes(names: &[String], query: &str) -> Vec<FindNode> {
    let patterns: Vec<_> = query.split('.').collect();
    let depth = patterns.len();
    let mut nodes = BTreeMap::<String, FindNode>::new();
    let mut unique_names = names.to_vec();
    unique_names.sort_unstable();
    unique_names.dedup();

    for series in unique_names {
        let segments: Vec<_> = series.split('.').collect();
        if segments.len() < depth
            || !patterns
                .iter()
                .zip(&segments)
                .all(|(pattern, segment)| segment_matches(pattern, segment))
        {
            continue;
        }

        let path = segments[..depth].join(".");
        let node = nodes.entry(path.clone()).or_insert_with(|| FindNode {
            path,
            name: segments[depth - 1].to_owned(),
            leaf: false,
            leaves: 0,
        });
        node.leaf |= segments.len() == depth;
        node.leaves += 1;
    }

    nodes.into_values().collect()
}

fn parse_time(value: &str, now: i64) -> Result<i64, &'static str> {
    if value == "now" {
        return Ok(now);
    }
    if let Some(relative) = value.strip_prefix('-') {
        let offset = parse_unit_duration(relative)?;
        return now
            .checked_sub(offset)
            .ok_or("relative time is out of range");
    }
    value.parse().map_err(|_| "invalid time")
}

fn parse_step(value: &str) -> Result<i64, &'static str> {
    let step = if value.bytes().all(|byte| byte.is_ascii_digit()) {
        value.parse().map_err(|_| "invalid step")?
    } else {
        parse_unit_duration(value).map_err(|_| "invalid step")?
    };
    if step > 0 {
        Ok(step)
    } else {
        Err("step must be greater than zero")
    }
}

fn parse_unit_duration(value: &str) -> Result<i64, &'static str> {
    let (number, multiplier) = match value.as_bytes().last() {
        Some(b's') => (&value[..value.len() - 1], 1_000),
        Some(b'm') => (&value[..value.len() - 1], 60_000),
        Some(b'h') => (&value[..value.len() - 1], 3_600_000),
        _ => return Err("invalid duration"),
    };
    let number = number.parse::<i64>().map_err(|_| "invalid duration")?;
    if number <= 0 {
        return Err("duration must be greater than zero");
    }
    number
        .checked_mul(multiplier)
        .ok_or("duration is out of range")
}

#[cfg(test)]
mod tests {
    use super::{FindNode, find_nodes, parse_step, parse_time};

    fn node(path: &str, leaf: bool, leaves: usize) -> FindNode {
        FindNode {
            path: path.to_owned(),
            name: path.rsplit('.').next().unwrap().to_owned(),
            leaf,
            leaves,
        }
    }

    #[test]
    fn parses_absolute_and_relative_times() {
        assert_eq!(parse_time("now", 10_000), Ok(10_000));
        assert_eq!(parse_time("1234", 10_000), Ok(1_234));
        assert_eq!(parse_time("-5s", 10_000), Ok(5_000));
        assert_eq!(parse_time("-15m", 1_000_000), Ok(100_000));
        assert_eq!(parse_time("-2h", 8_000_000), Ok(800_000));
    }

    #[test]
    fn rejects_invalid_times() {
        assert!(parse_time("later", 0).is_err());
        assert!(parse_time("-0s", 0).is_err());
        assert!(parse_time("-4d", 0).is_err());
    }

    #[test]
    fn parses_steps() {
        assert_eq!(parse_step("1000"), Ok(1_000));
        assert_eq!(parse_step("10s"), Ok(10_000));
        assert_eq!(parse_step("1m"), Ok(60_000));
        assert!(parse_step("0").is_err());
        assert!(parse_step("-1").is_err());
    }

    #[test]
    fn find_derives_nodes_across_mixed_depths() {
        let names = vec![
            "web.frontend.cpu".to_owned(),
            "web.frontend.requests".to_owned(),
            "web.backend.cpu.load".to_owned(),
            "demo.sine".to_owned(),
        ];

        assert_eq!(
            find_nodes(&names, "web.*"),
            vec![
                node("web.backend", false, 1),
                node("web.frontend", false, 2),
            ]
        );
    }

    #[test]
    fn find_marks_a_branch_that_is_also_a_leaf() {
        let names = vec!["web".to_owned(), "web.frontend.cpu".to_owned()];

        assert_eq!(find_nodes(&names, "web"), vec![node("web", true, 2)]);
    }

    #[test]
    fn find_counts_all_leaves_at_or_below_each_node() {
        let names = vec![
            "sys.cpu".to_owned(),
            "sys.cpu.load".to_owned(),
            "sys.cpu.temp".to_owned(),
            "sys.mem.used".to_owned(),
        ];

        assert_eq!(
            find_nodes(&names, "sys.*"),
            vec![node("sys.cpu", true, 3), node("sys.mem", false, 1)]
        );
    }
}
