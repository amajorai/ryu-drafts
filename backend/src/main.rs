//! `ryu-drafts` — the out-of-process drafts/outbox sidecar.
//!
//! Core spawns it (`kind: local`, sibling on `PATH` or `RYU_DRAFTS_BIN`),
//! health-checks it, and proxies `/api/drafts/*` to it on loopback, exactly like
//! `ryu-social` / `ryu-reasoning`. The store and the handlers live in the crate lib;
//! this binary is only the process shell around them.
//!
//! SECURITY: loopback-only bind (127.0.0.1) plus a shared-secret bearer gate
//! (`RYU_EXT_TOKEN`, injected by Core at spawn and re-stamped on every proxied hop).
//! Every `/api/drafts/*` route is protected and the gate is FAIL-CLOSED: with no
//! token configured, every protected route rejects with 401. `/health` is the one
//! un-gated route so Core's pre-auth probe succeeds; it returns no draft data.
//!
//! Port: `RYU_DRAFTS_PORT`, default 8012. Data: `$RYU_DIR/drafts/drafts.db`, so the
//! outbox lands under the same node directory Core uses.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::extract::Request;
use axum::http::{header::AUTHORIZATION, StatusCode};
use axum::middleware::{from_fn, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use ryu_drafts::api::{routes, Ctx};
use ryu_drafts::store::{data_dir, Store};

/// Default loopback port, kept identical to `apps-store/drafts/manifest.json`.
const DEFAULT_PORT: u16 = 8012;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("RYU_DRAFTS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let token = std::env::var("RYU_EXT_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    if token.is_none() {
        tracing::warn!(
            "ryu-drafts: no RYU_EXT_TOKEN set; every /api/drafts/* route is FAIL-CLOSED \
             (401) until Core spawns this sidecar with one"
        );
    }

    let store = Store::open(data_dir())?;
    let ctx = Arc::new(Ctx { store });

    // The headless dispatcher. Off when Core did not spawn us (no callback env), in
    // which case this process is a plain drafts store and armed drafts simply wait.
    ryu_drafts::dispatch::spawn(Arc::clone(&ctx));

    // `/openapi.json` rides INSIDE the same bearer gate as `/api/drafts/*`, at the
    // SERVER ROOT. Core fetches `http://127.0.0.1:<port>/openapi.json` on this
    // sidecar's first Healthy edge and lowers every operation it finds into searchable
    // LLM tools, so routing this one endpoint is what makes the whole `/api/drafts`
    // surface callable by an agent.
    //
    // Root, not under `/api/drafts`: Core tries the root FIRST, and keeping the
    // document off the mount keeps it out of the manifest's declared `http.routes[]` —
    // anything declared there is reachable through the generic ext-proxy, and the
    // schema is Core's to read, not an app surface. Inside the gate, not next to the
    // un-gated `/health`: Core stamps the injected `RYU_EXT_TOKEN` on the fetch, so the
    // gate costs the fetcher nothing — while un-gated it would disclose this app's
    // entire internal API surface to any other process on loopback.
    let protected = Router::new()
        .nest("/api/drafts", routes(ctx))
        .route(
            "/openapi.json",
            get(|| async { Json(ryu_drafts::api::openapi()) }),
        )
        .layer(from_fn(move |req: Request, next: Next| {
            let expected = token.clone();
            async move { bearer_gate(expected.as_deref(), req, next).await }
        }));

    let app = Router::new().route("/health", get(health)).merge(protected);

    // LOOPBACK ONLY (belt) + shared-secret bearer (suspenders): Core is the auth
    // front and re-stamps the bearer on the proxied hop.
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "ryu-drafts listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "ok": true, "service": "ryu-drafts" }))
}

/// Shared-secret bearer gate. Core stamps `authorization: Bearer <RYU_EXT_TOKEN>` on
/// the loopback hop, so a request that did NOT come through Core has no way to
/// present it. Fail-closed when no token is configured.
async fn bearer_gate(expected: Option<&str>, req: Request, next: Next) -> Response {
    let provided = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if bearer_ok(provided, expected) {
        return next.run(req).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "unauthorized" })),
    )
        .into_response()
}

/// Pure bearer check, factored out so the auth decision is unit-testable without a
/// server. Constant-time comparison: the token is a secret, and a length- or
/// prefix-sensitive compare leaks it a byte at a time.
fn bearer_ok(provided: Option<&str>, expected: Option<&str>) -> bool {
    let (Some(provided), Some(expected)) = (provided, expected) else {
        return false;
    };
    if provided.len() != expected.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in provided.bytes().zip(expected.bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gate_is_fail_closed_without_a_configured_token() {
        assert!(!bearer_ok(Some("anything"), None));
        assert!(!bearer_ok(None, None));
    }

    #[test]
    fn only_the_exact_token_passes() {
        assert!(bearer_ok(Some("s3cret"), Some("s3cret")));
        assert!(!bearer_ok(Some("s3cre"), Some("s3cret")));
        assert!(!bearer_ok(Some("s3cretx"), Some("s3cret")));
        assert!(!bearer_ok(None, Some("s3cret")));
    }
}
