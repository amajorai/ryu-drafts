//! `ryu-drafts` — the durable outbox for unsent messages.
//!
//! Three modules, one job each:
//!
//! - [`models`] — what a draft is and, in `Trigger::is_satisfied`, the one decision
//!   this app makes: may this draft go out now. Pure, so it is tested without a
//!   node, a clock or a socket.
//! - [`store`] — SQLite persistence and the claim protocol that keeps two desktop
//!   windows from sending the same draft twice.
//! - [`api`] — the HTTP surface Core proxies to.
//!
//! - [`dispatch`] — the loop that sends a ready draft, headless.
//!
//! A manifest sidecar holds no `RYU_TOKEN` (by design — see `sidecar/process.rs`),
//! so it cannot reach Core's chat API directly. It sends through two GRANTED kernel
//! capabilities instead: `node.readings` for the live facts, and `chat.startTurn`
//! to post the turn — the latter firewalled and, by default, routed through the
//! user's Approvals inbox. That is what lets a draft armed for "when my weekly
//! limit resets" go out at 04:00 with no window open.

pub mod api;
pub mod dispatch;
pub mod models;
pub mod store;
