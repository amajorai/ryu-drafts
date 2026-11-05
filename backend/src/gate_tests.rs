//! The sidecar's served router (`app_router`): `/api/drafts/*` and the root
//! `/openapi.json` sit behind the shared-secret bearer gate, which is fail-closed
//! without a configured token; `/health` stays open for Core's loopback probe.
//! Driven in-process through the real gate and merge order built for `main`.

use axum::body::{to_bytes, Body};
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use tower::ServiceExt;

use super::app_router;

const TOKEN: &str = "ext-token-123";
const API_PROBE: &str = "/api/drafts/gate-probe";
const OPENAPI: &str = "/openapi.json";

/// `app_router` with a stand-in API subrouter (mounted at `/gate-probe`, so it is
/// served under the real `/api/drafts` prefix) and a stand-in health route.
fn app(token: Option<&str>) -> Router {
    let health_route = Router::new().route("/health", get(|| async { "healthy" }));
    let api = Router::new().route("/gate-probe", get(|| async { "protected data" }));
    app_router(health_route, api, token.map(str::to_owned))
}

async fn send(app: &Router, uri: &str, authorization: Option<&str>) -> (StatusCode, Vec<u8>) {
    let mut request = Request::builder().uri(uri);
    if let Some(value) = authorization {
        request = request.header(AUTHORIZATION, value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).expect("request"))
        .await
        .expect("router is infallible");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, body.to_vec())
}

#[tokio::test]
async fn exact_bearer_reaches_the_api_and_the_openapi_document() {
    let app = app(Some(TOKEN));
    let bearer = format!("Bearer {TOKEN}");

    let (status, body) = send(&app, API_PROBE, Some(&bearer)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"protected data");

    let (status, body) = send(&app, OPENAPI, Some(&bearer)).await;
    assert_eq!(status, StatusCode::OK);
    let doc: serde_json::Value = serde_json::from_slice(&body).expect("openapi is JSON");
    assert!(doc["openapi"].is_string(), "not an OpenAPI document: {doc}");
    assert!(
        doc["paths"].is_object(),
        "OpenAPI document has no paths: {doc}"
    );
}

#[tokio::test]
async fn missing_wrong_or_non_bearer_credentials_get_401() {
    let app = app(Some(TOKEN));
    let wrong_token = "Bearer not-the-token".to_owned();
    let token_prefix = format!("Bearer {}", &TOKEN[..TOKEN.len() - 1]);
    let token_suffix = format!("Bearer {TOKEN}x");
    let basic = format!("Basic {TOKEN}");
    let lowercase_scheme = format!("bearer {TOKEN}");
    let bare = TOKEN.to_owned();
    let attempts = [
        None,
        Some("Bearer "),
        Some(wrong_token.as_str()),
        Some(token_prefix.as_str()),
        Some(token_suffix.as_str()),
        Some(basic.as_str()),
        Some(lowercase_scheme.as_str()),
        Some(bare.as_str()),
    ];
    for uri in [API_PROBE, OPENAPI] {
        for authorization in attempts {
            let (status, body) = send(&app, uri, authorization).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{uri} with {authorization:?} must be rejected"
            );
            assert!(
                !String::from_utf8_lossy(&body).contains("protected data"),
                "{uri} leaked the handler body"
            );
        }
    }
}

#[tokio::test]
async fn no_configured_token_fails_closed() {
    let bearer = format!("Bearer {TOKEN}");
    for configured in [None, Some("")] {
        let app = app(configured);
        for uri in [API_PROBE, OPENAPI] {
            for authorization in [None, Some("Bearer "), Some(bearer.as_str())] {
                let (status, _) = send(&app, uri, authorization).await;
                assert_eq!(
                    status,
                    StatusCode::UNAUTHORIZED,
                    "{uri} with token {configured:?} and {authorization:?} must be rejected"
                );
            }
        }
    }
}

#[tokio::test]
async fn health_is_open_without_a_bearer() {
    for configured in [None, Some(TOKEN)] {
        let (status, body) = send(&app(configured), "/health", None).await;
        assert_eq!(status, StatusCode::OK, "health with token {configured:?}");
        assert_eq!(body, b"healthy");
    }
}
