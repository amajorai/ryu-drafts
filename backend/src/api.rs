//! The HTTP surface, nested by `main` under `/api/drafts` and reached only through
//! Core's ext-proxy (`public_mount`), so every handler here is already
//! authenticated by the bearer gate in `main`.
//!
//! Two consumers, two shapes:
//!
//! - **`GET /list`** feeds the declarative `sidebar_sections` source. It returns
//!   [`DraftRow`]s — the draft plus `preview` / `state_label` / `waiting_for` —
//!   because a manifest `map` can only project keys that already exist.
//! - **`GET /queue`** feeds the desktop dispatcher. The dispatcher passes the
//!   node readings it already holds as query parameters and gets back the drafts
//!   whose conditions those readings satisfy. The readings are NOT stored: a
//!   pushed snapshot would go stale between the push and the poll, and the
//!   dispatcher is the only thing that can act on the answer anyway.

use std::sync::Arc;

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::models::{Draft, DraftRow, Readings, Settings, Source, State as DraftState, Trigger};
use crate::store::{now_ms, Store};

pub struct Ctx {
    pub store: Store,
}

pub fn routes(ctx: Arc<Ctx>) -> Router {
    Router::new()
        .route("/list", get(list))
        .route("/queue", get(queue))
        .route("/drafts", get(history).post(save).delete(delete_all))
        .route("/drafts/:id", get(get_one).patch(save_one).delete(delete_one))
        .route("/drafts/:id/arm", post(arm))
        .route("/drafts/:id/disarm", post(disarm))
        .route("/drafts/:id/claim", post(claim))
        .route("/drafts/:id/sent", post(sent))
        .route("/drafts/:id/failed", post(failed))
        .route("/settings", get(get_settings).put(put_settings))
        .with_state(ctx)
}

/// `GET /list` — every draft that is not already sent, newest first.
async fn list(State(ctx): State<Arc<Ctx>>) -> Response {
    match ctx.store.list_open() {
        Ok(drafts) => Json(json!({ "drafts": rows(drafts) })).into_response(),
        Err(e) => fail(e),
    }
}

/// `GET /queue?running=<n>&usage=<agent>:<percent>&…` — the drafts whose condition
/// the supplied readings satisfy, oldest first.
///
/// An omitted reading is UNKNOWN, not zero, and every trigger that depends on it
/// holds its draft (see [`Trigger::is_satisfied`]). So a dispatcher that could not
/// reach `/api/runs` this tick sends nothing rather than draining the queue into a
/// node it cannot see.
async fn queue(State(ctx): State<Arc<Ctx>>, RawQuery(q): RawQuery) -> Response {
    let readings = parse_readings(q.as_deref());
    match ctx.store.ready(&readings) {
        Ok(drafts) => Json(json!({ "drafts": rows(drafts) })).into_response(),
        Err(e) => fail(e),
    }
}

/// `GET /drafts?limit=<n>` — sent history, newest first.
async fn history(State(ctx): State<Arc<Ctx>>, RawQuery(q): RawQuery) -> Response {
    let limit = param(q.as_deref(), "limit")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(50)
        .clamp(1, 500);
    match ctx.store.list_sent(limit) {
        Ok(drafts) => Json(json!({ "drafts": rows(drafts) })).into_response(),
        Err(e) => fail(e),
    }
}

/// The body of a create/update. Every field optional but `text` so the composer can
/// autosave with nothing but the text and an id it chose.
#[derive(Debug, Deserialize)]
pub struct SaveBody {
    pub id: Option<String>,
    pub text: String,
    pub conversation_id: Option<String>,
    pub agent_id: Option<String>,
    pub model: Option<String>,
    pub folder_path: Option<String>,
    pub source: Option<String>,
    pub trigger: Option<Trigger>,
}

impl SaveBody {
    fn into_draft(self, id: String) -> Draft {
        let trigger = self.trigger.unwrap_or_default();
        let state = if trigger == Trigger::Manual {
            DraftState::Draft
        } else {
            DraftState::Armed
        };
        let now = now_ms();
        Draft {
            id,
            text: self.text,
            conversation_id: self.conversation_id,
            agent_id: self.agent_id,
            model: self.model,
            folder_path: self.folder_path,
            source: self.source.as_deref().map_or(Source::Manual, Source::parse),
            state,
            trigger,
            error: None,
            // A freshly saved draft has observed nothing yet. `Store::set_trigger`
            // also resets this on every arm, so re-arming starts the watch afresh.
            seen_busy: false,
            created_at: now,
            updated_at: now,
            claimed_at: None,
            sent_at: None,
        }
    }
}

/// `POST /drafts` — create or replace. The caller may supply the id, which is what
/// lets the composer keep autosaving into one draft rather than one per pause.
async fn save(State(ctx): State<Arc<Ctx>>, Json(body): Json<SaveBody>) -> Response {
    let id = body
        .id
        .clone()
        .filter(|id| is_valid_id(id))
        .unwrap_or_else(new_id);
    upsert(&ctx, body.into_draft(id))
}

/// `PATCH /drafts/:id` — same thing with the id in the path.
async fn save_one(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    Json(body): Json<SaveBody>,
) -> Response {
    if !is_valid_id(&id) {
        return bad_request("invalid draft id");
    }
    upsert(&ctx, body.into_draft(id))
}

fn upsert(ctx: &Ctx, draft: Draft) -> Response {
    match ctx.store.upsert(draft) {
        // `None` means the text was blank and the draft was removed (or never
        // created) — a successful outcome, not a missing resource.
        Ok(Some(d)) => Json(json!({ "draft": DraftRow::from(d) })).into_response(),
        Ok(None) => Json(json!({ "draft": serde_json::Value::Null })).into_response(),
        Err(e) => conflict(e),
    }
}

async fn get_one(State(ctx): State<Arc<Ctx>>, Path(id): Path<String>) -> Response {
    match ctx.store.get(&id) {
        Ok(Some(d)) => Json(json!({ "draft": DraftRow::from(d) })).into_response(),
        Ok(None) => not_found(),
        Err(e) => fail(e),
    }
}

async fn delete_one(State(ctx): State<Arc<Ctx>>, Path(id): Path<String>) -> Response {
    match ctx.store.delete(&id) {
        Ok(true) => Json(json!({ "deleted": true })).into_response(),
        Ok(false) => not_found(),
        Err(e) => fail(e),
    }
}

/// `DELETE /drafts` — wipe the outbox. Backs the manifest's `data_categories` entry.
async fn delete_all(State(ctx): State<Arc<Ctx>>) -> Response {
    match ctx.store.delete_all() {
        Ok(n) => Json(json!({ "deleted": n })).into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Debug, Deserialize)]
struct ArmBody {
    trigger: Trigger,
}

/// `POST /drafts/:id/arm` — give the draft a condition and queue it.
async fn arm(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    Json(body): Json<ArmBody>,
) -> Response {
    set_trigger(&ctx, &id, body.trigger)
}

/// `POST /drafts/:id/disarm` — back to an idle draft that sends only by hand.
async fn disarm(State(ctx): State<Arc<Ctx>>, Path(id): Path<String>) -> Response {
    set_trigger(&ctx, &id, Trigger::Manual)
}

fn set_trigger(ctx: &Ctx, id: &str, trigger: Trigger) -> Response {
    match ctx.store.set_trigger(id, trigger) {
        Ok(Some(d)) => Json(json!({ "draft": DraftRow::from(d) })).into_response(),
        Ok(None) => not_found(),
        Err(e) => conflict(e),
    }
}

/// `POST /drafts/:id/claim` — take the draft for sending, or 409 if someone already
/// has it. The dispatcher calls this BEFORE it sends; the 409 is what makes two
/// open windows safe.
async fn claim(State(ctx): State<Arc<Ctx>>, Path(id): Path<String>) -> Response {
    match ctx.store.claim(&id) {
        Ok(Some(d)) => Json(json!({ "draft": DraftRow::from(d) })).into_response(),
        Ok(None) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": "draft is already being sent" })),
        )
            .into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Debug, Deserialize)]
struct SentBody {
    conversation_id: Option<String>,
}

/// `POST /drafts/:id/sent` — the dispatcher reporting success, with the
/// conversation the message landed in so the history row can link to it.
async fn sent(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    body: Option<Json<SentBody>>,
) -> Response {
    let conversation_id = body.and_then(|Json(b)| b.conversation_id);
    match ctx.store.mark_sent(&id, conversation_id.as_deref()) {
        Ok(Some(d)) => Json(json!({ "draft": DraftRow::from(d) })).into_response(),
        Ok(None) => not_found(),
        Err(e) => fail(e),
    }
}

#[derive(Debug, Deserialize)]
struct FailedBody {
    error: Option<String>,
}

/// `POST /drafts/:id/failed` — the dispatcher reporting a failed send. The draft
/// comes back visible carrying the reason rather than vanishing.
async fn failed(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    body: Option<Json<FailedBody>>,
) -> Response {
    let reason = body
        .and_then(|Json(b)| b.error)
        .unwrap_or_else(|| "Send failed".to_owned());
    match ctx.store.mark_failed(&id, &reason) {
        Ok(Some(d)) => Json(json!({ "draft": DraftRow::from(d) })).into_response(),
        Ok(None) => not_found(),
        Err(e) => fail(e),
    }
}

async fn get_settings(State(ctx): State<Arc<Ctx>>) -> Response {
    match ctx.store.settings() {
        Ok(s) => Json(json!({ "settings": s })).into_response(),
        Err(e) => fail(e),
    }
}

async fn put_settings(State(ctx): State<Arc<Ctx>>, Json(body): Json<Settings>) -> Response {
    // A ceiling of 0 would queue every send forever with nothing able to release
    // it, so the floor is 1.
    let settings = Settings {
        max_concurrent: body.max_concurrent.max(1),
        ..body
    };
    match ctx.store.save_settings(&settings) {
        Ok(()) => Json(json!({ "settings": settings })).into_response(),
        Err(e) => fail(e),
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn rows(drafts: Vec<Draft>) -> Vec<DraftRow> {
    drafts.into_iter().map(DraftRow::from).collect()
}

fn new_id() -> String {
    format!("dft_{}", uuid::Uuid::new_v4().simple())
}

/// Draft ids arrive from clients and are used as primary keys and path segments, so
/// they are restricted to a charset with no separators.
pub fn is_valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Pull one value out of a raw query string.
fn param<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    query?.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then_some(v)
    })
}

/// Parse the dispatcher's readings out of the query string.
///
/// `running=<n>` and any number of `usage=<agent_id>:<used_percent>`. Raw parsing
/// rather than a `Deserialize` struct because `usage` repeats, and a malformed pair
/// is DROPPED rather than defaulted — a reading we cannot parse is a reading we do
/// not have, which holds the draft instead of releasing it on a zero.
pub fn parse_readings(query: Option<&str>) -> Readings {
    let mut readings = Readings {
        running: None,
        usage: Vec::new(),
        now_ms: now_ms(),
    };
    let Some(query) = query else {
        return readings;
    };
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        match key {
            "running" => readings.running = value.parse().ok(),
            "usage" => {
                // `agent:percent`. The agent id may itself contain a colon
                // (`acp:claude-code`), so split on the LAST one.
                let decoded = percent_decode(value);
                if let Some((agent, pct)) = decoded.rsplit_once(':') {
                    if let Ok(pct) = pct.parse::<f64>() {
                        if !agent.is_empty() {
                            readings.usage.push((agent.to_owned(), pct));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    readings
}

/// Minimal percent-decoding for the `usage` values, which carry agent ids that are
/// percent-encoded by the client (`acp%3Aclaude-code`). `+` is a space in a query
/// string; an invalid escape is left as written rather than dropping the reading.
fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "no such draft" })),
    )
        .into_response()
}

fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
}

/// A refused state transition (editing a draft the dispatcher is sending) — the
/// caller's request was well-formed, the draft was simply not available.
fn conflict(e: anyhow::Error) -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({ "error": e.to_string() })),
    )
        .into_response()
}

fn fail(e: anyhow::Error) -> Response {
    tracing::warn!(error = %e, "drafts request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": e.to_string() })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readings_default_to_unknown_when_nothing_is_supplied() {
        let r = parse_readings(None);
        assert!(r.running.is_none());
        assert!(r.usage.is_empty());
    }

    #[test]
    fn readings_parse_running_and_repeated_usage() {
        let r = parse_readings(Some("running=2&usage=claude:41.5&usage=codex:99"));
        assert_eq!(r.running, Some(2));
        assert_eq!(
            r.usage,
            vec![("claude".to_owned(), 41.5), ("codex".to_owned(), 99.0)]
        );
    }

    #[test]
    fn a_percent_encoded_agent_id_keeps_its_colon() {
        // `acp:claude-code` arrives as `acp%3Aclaude-code:12` — the split must be on
        // the LAST colon or the agent id is truncated to "acp".
        let r = parse_readings(Some("usage=acp%3Aclaude-code:12"));
        assert_eq!(r.usage, vec![("acp:claude-code".to_owned(), 12.0)]);
    }

    #[test]
    fn a_malformed_reading_is_dropped_not_defaulted() {
        // Defaulting to zero would look like "nothing is running" and drain the
        // whole queue.
        let r = parse_readings(Some("running=lots&usage=broken&usage=x:notanumber"));
        assert!(r.running.is_none());
        assert!(r.usage.is_empty());
    }

    #[test]
    fn ids_with_separators_are_refused() {
        assert!(is_valid_id("dft_abc-123"));
        assert!(!is_valid_id("../escape"));
        assert!(!is_valid_id("a/b"));
        assert!(!is_valid_id(""));
        assert!(!is_valid_id(&"x".repeat(65)));
    }
}
