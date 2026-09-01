//! The dispatcher: the loop that actually sends a queued draft.
//!
//! This lives HERE, in the sidecar, rather than in the desktop shell, and that is
//! the whole point of the app. A draft armed with "send when my weekly limit
//! resets" has to go out at whatever hour that happens — which is very often an
//! hour when no desktop is running. A dispatcher that needs an open window is a
//! queue that only drains while you are watching it.
//!
//! It is possible at all because of two kernel capabilities Core exposes to a
//! granted sidecar over loopback:
//!
//! - `node.readings` — the live facts (`running`, per-agent `used_percent`). A
//!   sidecar holds no node token, so it cannot read `/api/runs` or
//!   `/api/agents/:id/usage` itself.
//! - `chat.startTurn` — post the turn into a real conversation. Core scans the
//!   prompt through the exec firewall and, by default, queues it in the Approvals
//!   inbox for the user to confirm before a single token is spent.
//!
//! Both are authenticated by this process's minted `RYU_EXT_TOKEN` and gated on
//! grants this app declares in its manifest (`chat.sendFollowUp`, `core:readings`).
//! Neither can be reached by a process Core did not spawn.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::api::Ctx;
use crate::models::{Readings, State as DraftState};
use crate::store::now_ms;

/// How often the queue is evaluated. A draft waiting on a freed slot should start
/// within a few seconds of the slot freeing; polling faster only adds requests to a
/// node that is, by construction, already busy.
const TICK: Duration = Duration::from_secs(10);

/// Sends per tick. A queue that drained all at once would recreate exactly the
/// pile-up the concurrency trigger exists to prevent: every draft is judged against
/// the SAME reading, so ten drafts armed at "below 3" would all fire off one
/// observation of "2 running".
const MAX_SENDS_PER_TICK: usize = 1;

/// Bound on one loopback call, so a wedged Core cannot stall the tick forever.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Everything needed to call Core back, or `None` when this process was not spawned
/// by Core (a standalone run, a test) — in which case the dispatcher never starts
/// and the app is a plain drafts store.
struct HostCall {
    client: reqwest::Client,
    base: String,
    plugin_id: String,
    token: String,
}

impl HostCall {
    fn from_env() -> Option<HostCall> {
        let token = std::env::var("RYU_EXT_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())?;
        let plugin_id = std::env::var("RYU_EXT_PLUGIN_ID")
            .ok()
            .filter(|p| !p.is_empty())?;
        // Core's own (profile-shifted) loopback port, injected at spawn.
        let port = std::env::var("RYU_CORE_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())?;
        Some(HostCall {
            client: reqwest::Client::builder()
                .timeout(CALL_TIMEOUT)
                .build()
                .ok()?,
            base: format!("http://127.0.0.1:{port}"),
            plugin_id,
            token,
        })
    }

    /// POST one kernel capability. `Err` carries a message fit to show the user on
    /// a failed draft.
    async fn capability(&self, cap: &str, body: Value) -> Result<Value, String> {
        let resp = self
            .client
            .post(format!("{}/api/host/capability/{cap}", self.base))
            .header("x-ryu-plugin-id", &self.plugin_id)
            .header("authorization", format!("Bearer {}", self.token))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("could not reach Core: {e}"))?;
        let status = resp.status();
        let value: Value = resp.json().await.unwrap_or_else(|_| json!({}));
        if !status.is_success() {
            let msg = value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("call refused");
            return Err(format!("{cap}: {msg}"));
        }
        Ok(value)
    }
}

/// Start the dispatcher, unless this process has no way to call Core back.
pub fn spawn(ctx: Arc<Ctx>) {
    let Some(host) = HostCall::from_env() else {
        tracing::info!(
            "ryu-drafts: no Core host callback in the environment; the dispatcher is off and \
             armed drafts will wait (the store still works)"
        );
        return;
    };
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(TICK).await;
            if let Err(e) = tick(&ctx, &host).await {
                // A tick failing is normal (Core restarting, a vendor timeout); the
                // next one retries. Logged at debug so a long outage does not fill
                // the log with one line every ten seconds.
                tracing::debug!(error = %e, "ryu-drafts: dispatch tick failed");
            }
        }
    });
}

async fn tick(ctx: &Ctx, host: &HostCall) -> Result<(), String> {
    // Which readings are needed is decided by the queue itself: an outbox holding
    // only idle drafts asks Core for nothing, and only an armed `usage_reset` draft
    // costs a vendor round-trip.
    let open = ctx.store.list_open().map_err(|e| e.to_string())?;
    let armed: Vec<_> = open
        .iter()
        .filter(|d| d.state == DraftState::Armed)
        .collect();
    if armed.is_empty() {
        return Ok(());
    }
    let agent_ids: Vec<String> = {
        let mut ids: Vec<String> = armed
            .iter()
            .filter_map(|d| match &d.trigger {
                crate::models::Trigger::UsageReset { agent_id, .. } => Some(agent_id.clone()),
                _ => None,
            })
            .collect();
        ids.sort();
        ids.dedup();
        ids
    };

    let readings = host
        .capability("node.readings", json!({ "agent_ids": agent_ids }))
        .await?;
    // A reading Core did not return stays UNKNOWN, which holds the draft — never
    // defaulted to zero, which would read as "the node is idle" and drain the queue.
    let r = Readings {
        running: readings
            .get("running")
            .and_then(Value::as_u64)
            .map(|n| n as u32),
        usage: readings
            .get("usage")
            .and_then(Value::as_array)
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| {
                        Some((
                            row.get("agent_id")?.as_str()?.to_owned(),
                            row.get("used_percent")?.as_f64()?,
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default(),
        now_ms: now_ms(),
    };

    let ready = ctx.store.ready(&r).map_err(|e| e.to_string())?;
    for draft in ready.into_iter().take(MAX_SENDS_PER_TICK) {
        // Claim BEFORE sending. The desktop can dispatch too, and the claim is a
        // single conditional UPDATE, so exactly one of them wins this draft.
        let Some(claimed) = ctx.store.claim(&draft.id).map_err(|e| e.to_string())? else {
            continue;
        };
        let body = json!({
            "text": claimed.text,
            "agent_id": claimed.agent_id,
            "conversation_id": claimed.conversation_id,
            "model": claimed.model,
        });
        match host.capability("chat.startTurn", body).await {
            Ok(result) => {
                let conversation_id = result
                    .get("conversation_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                // `pending_approval` counts as SENT from the outbox's point of view:
                // the send now belongs to the Approvals inbox, which is where the
                // user acts on it. Leaving it armed would re-queue the same message
                // on the next tick and pile up duplicate approvals.
                let _ = ctx
                    .store
                    .mark_sent(&claimed.id, conversation_id.as_deref())
                    .map_err(|e| e.to_string())?;
            }
            Err(e) => {
                // Back to visible, carrying the reason — never silently dropped.
                let _ = ctx.store.mark_failed(&claimed.id, &e);
            }
        }
    }
    Ok(())
}
