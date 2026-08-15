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

/// The OpenAPI document Core fetches from `/openapi.json` and lowers into LLM tools.
///
/// The `#[utoipa::path]` annotations below carry the ABSOLUTE external path
/// (`/api/drafts/...`, `{id}` in brace form) while the router above registers paths
/// relative to the mount in axum's `:id` form. The two forms differ ON PURPOSE — Core
/// nests this router at `/api/drafts`, and the document has to describe the URL a
/// caller actually hits. Do not "align" either side.
///
/// One thing the summaries are careful about, because the route names invite the wrong
/// reading: **nothing here sends anything**. This sidecar has no way to start a turn
/// (see the crate docs); the desktop dispatcher does the sending and then calls `sent`
/// or `failed` to report what happened. `claim` likewise reserves a draft, it does not
/// deliver it.
#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    <DraftsApiDoc as utoipa::OpenApi>::openapi()
}

/// `components(schemas(...))` is what turns each `request_body = T` into a resolvable
/// `#/components/schemas/T`: without the entry the operation still carries a `$ref`
/// whose target is missing, and Core derives a write tool with zero visible arguments —
/// discoverable and uncallable. utoipa 5 also auto-collects schemas reachable from the
/// annotated paths, so these rows are belt-and-braces; they are listed anyway so the
/// registration is greppable and cannot be lost to an attribute edit.
///
/// `Trigger` is here because it is reachable only TRANSITIVELY, through `SaveBody` and
/// `ArmBody`.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        list,
        queue,
        history,
        save,
        delete_all,
        get_one,
        save_one,
        delete_one,
        arm,
        disarm,
        claim,
        sent,
        failed,
        get_settings,
        put_settings,
    ),
    components(schemas(SaveBody, ArmBody, SentBody, FailedBody, Trigger, Settings))
)]
struct DraftsApiDoc;

/// `GET /list` — every draft that is not already sent, newest first.
#[utoipa::path(
    get,
    path = "/api/drafts/list",
    tag = "Drafts",
    summary = "List every unsent draft in the outbox, newest first, with what each one is waiting for.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
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
// The handler takes `RawQuery` because `usage` REPEATS and a `Deserialize` struct
// cannot express that; the params below describe the same query string for the
// document's benefit.
#[utoipa::path(
    get,
    path = "/api/drafts/queue",
    tag = "Drafts",
    summary = "Ask which armed drafts are releasable given the node readings you pass in. Reports readiness; it does not send.",
    params(
        ("running" = Option<u32>, Query, description = "How many agent runs are active right now. Omit it and every draft waiting on concurrency is held rather than released."),
        ("usage" = Option<String>, Query, description = "One agent's fullest usage window as `agent_id:used_percent` (percent-encode a colon in the id). Repeat the parameter once per agent."),
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn queue(State(ctx): State<Arc<Ctx>>, RawQuery(q): RawQuery) -> Response {
    let readings = parse_readings(q.as_deref());
    match ctx.store.ready(&readings) {
        Ok(drafts) => Json(json!({ "drafts": rows(drafts) })).into_response(),
        Err(e) => fail(e),
    }
}

/// `GET /drafts?limit=<n>` — sent history, newest first.
#[utoipa::path(
    get,
    path = "/api/drafts/drafts",
    tag = "Drafts",
    summary = "List drafts that have already been sent, newest first.",
    params(("limit" = Option<u32>, Query, description = "How many rows to return, 1–500. Defaults to 50.")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
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
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SaveBody {
    /// Id to write under. Supply the id of an existing draft to replace it; omit it
    /// for a new one. Letters, digits, `-` and `_` only, up to 64 characters.
    pub id: Option<String>,
    /// The message text. Saving a blank text deletes the draft rather than storing an
    /// empty one.
    pub text: String,
    /// The conversation this text belongs to. A draft with no conversation is sent as
    /// a NEW chat when it eventually goes out.
    pub conversation_id: Option<String>,
    /// Which agent should receive it.
    pub agent_id: Option<String>,
    /// Model override for the eventual send.
    pub model: Option<String>,
    /// Working folder for the eventual send.
    pub folder_path: Option<String>,
    /// Where the draft came from: `manual`, `composer-autosave` or `auto-queue`.
    /// Anything else is recorded as `manual`.
    pub source: Option<String>,
    /// The condition that releases it. Omit (or send `{"kind":"manual"}`) for a draft
    /// that only ever goes out when someone asks for it; supplying anything else arms
    /// the draft immediately.
    // Inlined so the model reads the five real condition shapes instead of a `$ref` it
    // cannot follow — Core resolves refs only one level into a schema, so a
    // property-level reference reaches the model as an empty object.
    #[schema(inline)]
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
#[utoipa::path(
    post,
    path = "/api/drafts/drafts",
    tag = "Drafts",
    summary = "Save a message into the outbox for later, optionally armed with the condition that should release it. Storing only; nothing is sent.",
    request_body = SaveBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn save(State(ctx): State<Arc<Ctx>>, Json(body): Json<SaveBody>) -> Response {
    let id = body
        .id
        .clone()
        .filter(|id| is_valid_id(id))
        .unwrap_or_else(new_id);
    upsert(&ctx, body.into_draft(id))
}

/// `PATCH /drafts/:id` — same thing with the id in the path.
#[utoipa::path(
    patch,
    path = "/api/drafts/drafts/{id}",
    tag = "Drafts",
    summary = "Rewrite one draft in place, keeping its id.",
    params(("id" = String, Path, description = "Id of the draft to rewrite")),
    request_body = SaveBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
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

#[utoipa::path(
    get,
    path = "/api/drafts/drafts/{id}",
    tag = "Drafts",
    summary = "Read one draft — its full text, its state, and what it is waiting for.",
    params(("id" = String, Path, description = "Id of the draft to read")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_one(State(ctx): State<Arc<Ctx>>, Path(id): Path<String>) -> Response {
    match ctx.store.get(&id) {
        Ok(Some(d)) => Json(json!({ "draft": DraftRow::from(d) })).into_response(),
        Ok(None) => not_found(),
        Err(e) => fail(e),
    }
}

#[utoipa::path(
    delete,
    path = "/api/drafts/drafts/{id}",
    tag = "Drafts",
    summary = "Discard one draft.",
    params(("id" = String, Path, description = "Id of the draft to discard")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_one(State(ctx): State<Arc<Ctx>>, Path(id): Path<String>) -> Response {
    match ctx.store.delete(&id) {
        Ok(true) => Json(json!({ "deleted": true })).into_response(),
        Ok(false) => not_found(),
        Err(e) => fail(e),
    }
}

/// `DELETE /drafts` — wipe the outbox. Backs the manifest's `data_categories` entry.
#[utoipa::path(
    delete,
    path = "/api/drafts/drafts",
    tag = "Drafts",
    summary = "Wipe the entire outbox, including sent history. Irreversible.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_all(State(ctx): State<Arc<Ctx>>) -> Response {
    match ctx.store.delete_all() {
        Ok(n) => Json(json!({ "deleted": n })).into_response(),
        Err(e) => fail(e),
    }
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct ArmBody {
    /// The condition that releases this draft. `concurrency` waits for the node to
    /// drop below a run count, `all_done` waits for work to start and then finish,
    /// `usage_reset` waits for an agent's quota window to have room again, `at` waits
    /// for a wall-clock instant, and `manual` disarms it.
    // Inlined for the same reason as `SaveBody::trigger`: a property-level `$ref` is
    // one level too deep for Core to resolve, and the model would see an empty object
    // where the five condition shapes should be.
    #[schema(inline)]
    trigger: Trigger,
}

/// `POST /drafts/:id/arm` — give the draft a condition and queue it.
#[utoipa::path(
    post,
    path = "/api/drafts/drafts/{id}/arm",
    tag = "Drafts",
    summary = "Queue a draft behind a condition — under a run count, after the node goes quiet, once a quota window resets, or at a set time.",
    params(("id" = String, Path, description = "Id of the draft to arm")),
    request_body = ArmBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn arm(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    Json(body): Json<ArmBody>,
) -> Response {
    set_trigger(&ctx, &id, body.trigger)
}

/// `POST /drafts/:id/disarm` — back to an idle draft that sends only by hand.
#[utoipa::path(
    post,
    path = "/api/drafts/drafts/{id}/disarm",
    tag = "Drafts",
    summary = "Take a queued draft back off its condition, so it waits for a person instead.",
    params(("id" = String, Path, description = "Id of the draft to disarm")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
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
// Summary written for the reader who assumes "claim" means "deliver". It does not:
// this sidecar cannot start a turn at all. Claiming a draft nobody then sends leaves
// it reserved until the claim times out.
#[utoipa::path(
    post,
    path = "/api/drafts/drafts/{id}/claim",
    tag = "Drafts",
    summary = "Reserve a draft so no other window sends it. Reserves only — this does not deliver the message.",
    params(("id" = String, Path, description = "Id of the draft to reserve")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
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

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct SentBody {
    /// The conversation the message actually landed in, so the history row can link
    /// to it. Omit if the send did not produce one.
    conversation_id: Option<String>,
}

/// `POST /drafts/:id/sent` — the dispatcher reporting success, with the
/// conversation the message landed in so the history row can link to it.
// This is BOOKKEEPING, not a send: it files an already-completed delivery. Calling it
// on a draft nobody delivered marks the message sent and hides it, which is precisely
// the way to lose one — hence the blunt summary.
#[utoipa::path(
    post,
    path = "/api/drafts/drafts/{id}/sent",
    tag = "Drafts",
    summary = "Record that a draft was already delivered elsewhere and file it in history. This sends nothing; it only marks a completed send.",
    params(("id" = String, Path, description = "Id of the draft that was delivered")),
    request_body = SentBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
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

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct FailedBody {
    /// Why the send failed, shown on the draft row. Defaults to "Send failed".
    error: Option<String>,
}

/// `POST /drafts/:id/failed` — the dispatcher reporting a failed send. The draft
/// comes back visible carrying the reason rather than vanishing.
#[utoipa::path(
    post,
    path = "/api/drafts/drafts/{id}/failed",
    tag = "Drafts",
    summary = "Record that a delivery attempt failed and put the draft back in the outbox with the reason attached.",
    params(("id" = String, Path, description = "Id of the draft whose delivery failed")),
    request_body = FailedBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
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

#[utoipa::path(
    get,
    path = "/api/drafts/settings",
    tag = "Drafts",
    summary = "Read the node-wide outbox settings: autosave, auto-queue and the concurrency ceiling.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_settings(State(ctx): State<Arc<Ctx>>) -> Response {
    match ctx.store.settings() {
        Ok(s) => Json(json!({ "settings": s })).into_response(),
        Err(e) => fail(e),
    }
}

#[utoipa::path(
    put,
    path = "/api/drafts/settings",
    tag = "Drafts",
    summary = "Replace the node-wide outbox settings. Changing auto-queue changes what pressing Enter does, so ask before turning it on.",
    request_body = Settings,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
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

    // ── OpenAPI document ───────────────────────────────────────────────────────

    /// This app's own manifest, read at compile time. The route contract lives there,
    /// so the invariants below compare the document against the real declaration
    /// rather than against a second list that could drift from it.
    fn manifest() -> serde_json::Value {
        serde_json::from_str(include_str!("../../manifest.json")).expect("valid JSON")
    }

    /// The sidecar that declares an `http.mount` — selected by mount rather than by
    /// index so a future mountless sidecar cannot silently redirect the assertion.
    fn mounted_sidecar() -> serde_json::Value {
        manifest()["sidecars"]
            .as_array()
            .expect("sidecars must be an array")
            .iter()
            .find(|s| s["http"]["mount"].is_string())
            .expect("one sidecar must declare an http.mount")
            .clone()
    }

    /// A manifest route (relative to the mount, in axum's `:param` form) rewritten into
    /// the form the OpenAPI document uses (absolute, `{param}`). The two forms differ
    /// deliberately; normalise here rather than "aligning" either side.
    fn doc_path_for(mount: &str, route: &str) -> String {
        let joined = if route == "/" {
            mount.to_owned()
        } else {
            format!("{mount}{route}")
        };
        joined
            .split('/')
            .map(|seg| match seg.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => seg.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    #[test]
    fn openapi_doc_covers_the_served_routes() {
        let doc = openapi();
        assert!(!doc.paths.paths.is_empty());
    }

    #[test]
    fn every_declared_route_appears_in_the_openapi_doc() {
        // The direction that decides tool yield. Core keeps only the document
        // operations the manifest ALSO declares, so a declared route with no
        // `#[utoipa::path]` annotation is a tool that silently never exists.
        let sidecar = mounted_sidecar();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        let doc = openapi();
        for route in sidecar["http"]["routes"]
            .as_array()
            .expect("routes must be an array")
        {
            let path = route["path"].as_str().expect("a route path");
            let expected = doc_path_for(mount, path);
            assert!(
                doc.paths.paths.contains_key(&expected),
                "'{path}' is declared in manifest.json but the OpenAPI document has no \
                 '{expected}' operation — Core derives no tool for it"
            );
        }
    }

    #[test]
    fn arming_a_draft_exposes_the_real_trigger_shapes_not_an_opaque_ref() {
        // Core resolves a `$ref` only one level into a schema, so a property-level
        // reference to `Trigger` would reach the model as an empty object and `arm`
        // would be uncallable. `#[schema(inline)]` is what prevents that; this pins it.
        // Read the COMPONENT, which is the one level Core does resolve: the operation
        // itself only ever carries `$ref: ArmBody`.
        let doc = serde_json::to_value(openapi()).expect("the document serializes");
        let rendered = doc["components"]["schemas"]["ArmBody"].to_string();
        assert!(
            rendered.contains("usage_reset") && rendered.contains("below_percent"),
            "the arm body does not spell out the trigger variants: {rendered}"
        );
    }

    #[test]
    fn every_write_route_documents_a_typed_request_body() {
        // An untyped body lowers to a tool with zero visible arguments: the model can
        // see it and can never fill it in.
        let doc = serde_json::to_value(openapi()).expect("the document serializes");
        for (path, verb) in [
            ("/api/drafts/drafts", "post"),
            ("/api/drafts/drafts/{id}", "patch"),
            ("/api/drafts/drafts/{id}/arm", "post"),
            ("/api/drafts/drafts/{id}/sent", "post"),
            ("/api/drafts/drafts/{id}/failed", "post"),
            ("/api/drafts/settings", "put"),
        ] {
            let schema = &doc["paths"][path][verb]["requestBody"]["content"]
                ["application/json"]["schema"];
            let named = schema["$ref"].is_string() || schema["properties"].is_object();
            assert!(named, "{verb} {path} has no typed request body: {schema}");
        }
    }
}
