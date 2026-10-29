//! The headless dispatcher's `tick`, driven against a loopback stand-in for Core's
//! kernel-capability broker (`/api/host/capability/{cap}`). The stub records every
//! request (path, auth headers, JSON body) and answers per capability, so these
//! tests pin what the sidecar asks Core for and what it does with the answer.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use super::{tick, HostCall};
use crate::api::Ctx;
use crate::models::{Draft, Source, State as DraftState, Trigger};
use crate::store::{Store, INTERRUPTED_SEND_ERROR};

const TOKEN: &str = "ext-token-xyz";
const PLUGIN_ID: &str = "@ryu/drafts";

#[derive(Debug, Clone)]
struct Seen {
    cap: String,
    auth: Option<String>,
    plugin_id: Option<String>,
    body: Value,
}

#[derive(Clone)]
struct Stub {
    replies: Arc<HashMap<String, (u16, Value)>>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

async fn capability(
    Path(cap): Path<String>,
    headers: HeaderMap,
    State(stub): State<Stub>,
    Json(body): Json<Value>,
) -> Response {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    stub.seen.lock().unwrap().push(Seen {
        cap: cap.clone(),
        auth: header("authorization"),
        plugin_id: header("x-ryu-plugin-id"),
        body,
    });
    let (status, reply) = stub
        .replies
        .get(&cap)
        .cloned()
        .unwrap_or((404, json!({ "error": "unknown capability" })));
    (StatusCode::from_u16(status).unwrap(), Json(reply)).into_response()
}

/// A Core stand-in on a random loopback port, plus the `HostCall` pointed at it.
async fn stub_core(replies: &[(&str, u16, Value)]) -> (HostCall, Arc<Mutex<Vec<Seen>>>) {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let stub = Stub {
        replies: Arc::new(
            replies
                .iter()
                .map(|(cap, status, body)| ((*cap).to_owned(), (*status, body.clone())))
                .collect(),
        ),
        seen: seen.clone(),
    };
    let app = Router::new()
        .route("/api/host/capability/:cap", post(capability))
        .with_state(stub);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let host = HostCall {
        client: reqwest::Client::new(),
        base: format!("http://127.0.0.1:{port}"),
        plugin_id: PLUGIN_ID.to_owned(),
        token: TOKEN.to_owned(),
    };
    (host, seen)
}

fn ctx() -> Ctx {
    Ctx {
        store: Store::memory().expect("store"),
    }
}

fn draft(id: &str, text: &str, state: DraftState) -> Draft {
    Draft {
        id: id.to_owned(),
        text: text.to_owned(),
        conversation_id: None,
        agent_id: Some("agent-7".to_owned()),
        model: Some("model-x".to_owned()),
        folder_path: None,
        source: Source::Manual,
        state,
        trigger: Trigger::Manual,
        error: None,
        seen_busy: false,
        created_at: 0,
        updated_at: 0,
        claimed_at: None,
        sent_at: None,
    }
}

fn armed(ctx: &Ctx, id: &str, trigger: Trigger) {
    ctx.store
        .upsert(draft(id, &format!("prompt for {id}"), DraftState::Draft))
        .unwrap();
    ctx.store.set_trigger(id, trigger).unwrap();
}

fn state_of(ctx: &Ctx, id: &str) -> Draft {
    ctx.store.get(id).unwrap().expect("draft exists")
}

#[tokio::test]
async fn idle_outbox_asks_core_for_nothing_but_still_releases_a_stranded_claim() {
    let ctx = ctx();
    let (host, seen) = stub_core(&[
        ("node.readings", 200, json!({ "running": 0 })),
        ("chat.startTurn", 200, json!({ "conversation_id": "c" })),
    ])
    .await;
    // An idle draft, and one left in `sending` (no claim time) by a dispatcher that
    // died: the only open work, so the early return must not skip releasing it.
    ctx.store
        .upsert(draft("idle", "just typing", DraftState::Draft))
        .unwrap();
    ctx.store
        .upsert(draft("stranded", "half sent", DraftState::Sending))
        .unwrap();

    tick(&ctx, &host).await.unwrap();

    assert!(
        seen.lock().unwrap().is_empty(),
        "no armed draft, no Core call"
    );
    let stranded = state_of(&ctx, "stranded");
    assert_eq!(stranded.state, DraftState::Failed);
    assert_eq!(stranded.error.as_deref(), Some(INTERRUPTED_SEND_ERROR));
    assert_eq!(state_of(&ctx, "idle").state, DraftState::Draft);
}

#[tokio::test]
async fn readings_request_is_authenticated_and_carries_sorted_unique_usage_agents() {
    let ctx = ctx();
    // Core answers with no `running` and no usage rows: every fact is unknown.
    let (host, seen) = stub_core(&[
        ("node.readings", 200, json!({})),
        ("chat.startTurn", 200, json!({ "conversation_id": "c" })),
    ])
    .await;
    let usage = |agent: &str| Trigger::UsageReset {
        agent_id: agent.to_owned(),
        below_percent: 50.0,
    };
    armed(&ctx, "u1", usage("zeta"));
    armed(&ctx, "u2", usage("alpha"));
    armed(&ctx, "u3", usage("zeta"));
    armed(&ctx, "slots", Trigger::Concurrency { below: 3 });

    tick(&ctx, &host).await.unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        1,
        "unknown readings must not reach chat.startTurn"
    );
    assert_eq!(seen[0].cap, "node.readings");
    assert_eq!(seen[0].body, json!({ "agent_ids": ["alpha", "zeta"] }));
    assert_eq!(seen[0].plugin_id.as_deref(), Some(PLUGIN_ID));
    assert_eq!(
        seen[0].auth.as_deref(),
        Some(format!("Bearer {TOKEN}").as_str())
    );
    // A missing `running` is UNKNOWN, never zero: the concurrency draft holds.
    for id in ["u1", "u2", "u3", "slots"] {
        assert_eq!(state_of(&ctx, id).state, DraftState::Armed, "{id} held");
    }
}

#[tokio::test]
async fn sends_one_ready_draft_per_tick_and_records_the_conversation() {
    let ctx = ctx();
    let (host, seen) = stub_core(&[
        ("node.readings", 200, json!({ "running": 0 })),
        (
            "chat.startTurn",
            200,
            json!({ "conversation_id": "conv-42", "status": "pending_approval" }),
        ),
    ])
    .await;
    armed(&ctx, "first", Trigger::Concurrency { below: 3 });
    armed(&ctx, "second", Trigger::Concurrency { below: 3 });

    tick(&ctx, &host).await.unwrap();

    let calls = seen.lock().unwrap().clone();
    let turns: Vec<&Seen> = calls.iter().filter(|s| s.cap == "chat.startTurn").collect();
    assert_eq!(turns.len(), 1, "MAX_SENDS_PER_TICK is one");
    assert_eq!(turns[0].plugin_id.as_deref(), Some(PLUGIN_ID));
    assert_eq!(
        turns[0].auth.as_deref(),
        Some(format!("Bearer {TOKEN}").as_str())
    );
    let sent_text = turns[0].body["text"].as_str().unwrap().to_owned();
    let sent_id = sent_text.strip_prefix("prompt for ").unwrap().to_owned();
    assert_eq!(turns[0].body["agent_id"], "agent-7");
    assert_eq!(turns[0].body["model"], "model-x");
    assert!(turns[0].body["conversation_id"].is_null());

    let sent = state_of(&ctx, &sent_id);
    assert_eq!(sent.state, DraftState::Sent);
    assert_eq!(sent.conversation_id.as_deref(), Some("conv-42"));
    let other = if sent_id == "first" {
        "second"
    } else {
        "first"
    };
    assert_eq!(state_of(&ctx, other).state, DraftState::Armed);

    // The next tick drains the remaining one.
    tick(&ctx, &host).await.unwrap();
    assert_eq!(state_of(&ctx, other).state, DraftState::Sent);
}

#[tokio::test]
async fn refused_start_turn_fails_the_draft_with_the_capability_and_reason() {
    let ctx = ctx();
    let (host, _seen) = stub_core(&[
        ("node.readings", 200, json!({ "running": 1 })),
        ("chat.startTurn", 403, json!({ "error": "grant not held" })),
    ])
    .await;
    armed(&ctx, "denied", Trigger::Concurrency { below: 2 });

    tick(&ctx, &host).await.unwrap();

    let denied = state_of(&ctx, "denied");
    assert_eq!(denied.state, DraftState::Failed);
    assert_eq!(
        denied.error.as_deref(),
        Some("chat.startTurn: grant not held")
    );
    assert!(denied.sent_at.is_none());
}

#[tokio::test]
async fn refused_readings_abort_the_tick_and_leave_the_queue_armed() {
    let ctx = ctx();
    let (host, seen) = stub_core(&[
        ("node.readings", 503, json!({})),
        ("chat.startTurn", 200, json!({ "conversation_id": "c" })),
    ])
    .await;
    armed(&ctx, "waiting", Trigger::Concurrency { below: 5 });

    let err = tick(&ctx, &host).await.unwrap_err();

    assert_eq!(err, "node.readings: call refused");
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(state_of(&ctx, "waiting").state, DraftState::Armed);
}
