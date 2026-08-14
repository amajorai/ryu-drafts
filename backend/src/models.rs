//! The draft vocabulary: what a draft is, what it can be waiting for, and the
//! node-wide settings that decide when one is created without being asked for.
//!
//! Everything here is data. The decision "may this draft go out now" lives in
//! [`Trigger::is_satisfied`] and is a pure function of the draft plus a
//! [`Readings`] snapshot, so it is testable without a node, a clock, or a network.

use serde::{Deserialize, Serialize};

/// Where a draft came from. Kept because it changes how the row should read: text
/// the user abandoned is not the same thing as a prompt they deliberately queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Source {
    /// Composer text the user walked away from without sending.
    ComposerAutosave,
    /// Written directly in the Drafts companion.
    Manual,
    /// A send that was turned into a draft because the node was already at its
    /// concurrency ceiling.
    AutoQueue,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::ComposerAutosave => "composer-autosave",
            Source::Manual => "manual",
            Source::AutoQueue => "auto-queue",
        }
    }

    pub fn parse(raw: &str) -> Source {
        match raw {
            "composer-autosave" => Source::ComposerAutosave,
            "auto-queue" => Source::AutoQueue,
            _ => Source::Manual,
        }
    }
}

/// A draft's position in the outbox.
///
/// `Sending` is a CLAIM, not a phase of an HTTP request: the dispatcher that is
/// about to send takes it, so a second desktop window polling the same queue does
/// not send the same draft again. A claim carries a timestamp and expires
/// ([`crate::store::CLAIM_TIMEOUT_MS`]) so a dispatcher that dies mid-send releases
/// the draft instead of stranding it forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    /// Idle. Never sends itself.
    Draft,
    /// Has a condition and is waiting for it.
    Armed,
    /// Claimed by a dispatcher that is sending it right now.
    Sending,
    /// Sent. Kept as history; hidden from the sidebar.
    Sent,
    /// A send was attempted and failed. Visible again, with the error.
    Failed,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Draft => "draft",
            State::Armed => "armed",
            State::Sending => "sending",
            State::Sent => "sent",
            State::Failed => "failed",
        }
    }

    pub fn parse(raw: &str) -> State {
        match raw {
            "armed" => State::Armed,
            "sending" => State::Sending,
            "sent" => State::Sent,
            "failed" => State::Failed,
            _ => State::Draft,
        }
    }

    /// The human label the sidebar row shows.
    pub fn label(self) -> &'static str {
        match self {
            State::Draft => "Draft",
            State::Armed => "Queued",
            State::Sending => "Sending",
            State::Sent => "Sent",
            State::Failed => "Failed to send",
        }
    }
}

/// What an armed draft is waiting for.
///
/// Deliberately a CLOSED set of two real conditions plus a wall-clock time. Every
/// one of them is decidable from readings the desktop already takes for its own
/// surfaces (`/api/runs`, `/api/agents/:id/usage`), which is the whole reason the
/// set is this small: a condition nobody can evaluate is a draft that never sends,
/// and a queue that silently never drains is worse than no queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Trigger {
    /// Never sends itself. The default, and what a draft falls back to when it is
    /// disarmed.
    Manual,
    /// Send once fewer than `below` agent runs are active on this node.
    Concurrency { below: u32 },
    /// Send once the node goes QUIET — every agent run has finished.
    ///
    /// Not a synonym for `Concurrency { below: 1 }`, and the difference is the whole
    /// reason this variant exists. `below: 1` is a pure statement about the present:
    /// arm it on an idle node and it fires on the very next tick, because nothing is
    /// running *right now*. That is exactly wrong for what this is for — "post the
    /// release prompt once my agents have finished" queued while the work is still
    /// being set up would go out immediately, before any of it ran.
    ///
    /// So this one is a statement about a TRANSITION: the node must be seen busy,
    /// and then seen quiet. The "seen busy" half is latched per draft
    /// (`Draft::seen_busy`) by the dispatcher, which is why this arm needs that flag
    /// passed in while every other arm is a pure function of the readings.
    ///
    /// Consequence, deliberate: a draft armed this way on a node that never gets
    /// busy waits forever rather than firing. That is the honest behaviour — it was
    /// queued to follow some work, and there has been no work to follow.
    AllDone,
    /// Send once `agent_id`'s fullest usage window is back under `below_percent`.
    ///
    /// Expressed as "has room again" rather than "the window reset at T" because
    /// `resets_at` is `Option` — vendors report it inconsistently — while
    /// `used_percent` is always present. A reset shows up as the percentage
    /// falling, so this fires on the observable fact rather than on a promised
    /// time that may never arrive.
    UsageReset { agent_id: String, below_percent: f64 },
    /// Send at a wall-clock instant (epoch millis, UTC).
    At { epoch_ms: i64 },
}

impl Default for Trigger {
    fn default() -> Self {
        Trigger::Manual
    }
}

/// One reading of the node, taken by the dispatcher and passed in with the queue
/// poll. Facts only — no policy.
#[derive(Debug, Clone, Default)]
pub struct Readings {
    /// How many agent runs are active right now, when the dispatcher could read it.
    /// `None` means "unknown", which makes every [`Trigger::Concurrency`] hold its
    /// draft rather than guess.
    pub running: Option<u32>,
    /// `(agent_id, fullest window's used_percent)` for every agent the dispatcher
    /// could read. An agent absent here is unknown, and its drafts wait.
    pub usage: Vec<(String, f64)>,
    /// Now, in epoch millis.
    pub now_ms: i64,
}

impl Trigger {
    /// Whether this draft may be sent given `r`.
    ///
    /// Fail-CLOSED on every missing reading: an unknown fact holds the draft. The
    /// cost of holding is that a message goes out later than it could; the cost of
    /// firing on an unknown is a message sent into a node that is still capped,
    /// which is the failure the queue exists to prevent.
    ///
    /// `seen_busy` is the draft's own latch — whether this node has been observed
    /// with work running since the draft was armed. Only [`Trigger::AllDone`] reads
    /// it; every other arm is a pure function of the readings.
    pub fn is_satisfied(&self, r: &Readings, seen_busy: bool) -> bool {
        match self {
            Trigger::Manual => false,
            Trigger::Concurrency { below } => {
                r.running.is_some_and(|running| running < *below)
            }
            // Both halves required: the work must have STARTED (latched) and then
            // finished. Without the latch this fires the instant it is armed on an
            // idle node.
            Trigger::AllDone => seen_busy && r.running == Some(0),
            Trigger::UsageReset {
                agent_id,
                below_percent,
            } => r
                .usage
                .iter()
                .find(|(id, _)| id == agent_id)
                .is_some_and(|(_, used)| *used < *below_percent),
            Trigger::At { epoch_ms } => r.now_ms >= *epoch_ms,
        }
    }

    /// The one-line "waiting for" the sidebar row shows under the preview.
    pub fn waiting_for(&self) -> String {
        match self {
            Trigger::Manual => String::new(),
            Trigger::Concurrency { below } => match below {
                1 => "Waiting for nothing to be running".to_owned(),
                n => format!("Waiting for fewer than {n} runs"),
            },
            // Deliberately NOT parameterised on the latch here: `waiting_for` has
            // only the trigger. `DraftRow` overrides this line for an AllDone draft
            // that has not latched yet, because "waiting for every agent to finish"
            // on a node where nothing ever started reads as broken.
            Trigger::AllDone => "Waiting for every agent to finish".to_owned(),
            Trigger::UsageReset {
                agent_id,
                below_percent,
            } => format!("Waiting for {agent_id} to drop under {below_percent:.0}% used"),
            Trigger::At { epoch_ms } => match chrono::DateTime::from_timestamp_millis(*epoch_ms) {
                Some(t) => format!("Sending at {}", t.format("%Y-%m-%d %H:%M UTC")),
                None => "Sending at a set time".to_owned(),
            },
        }
    }
}

/// One draft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Draft {
    pub id: String,
    /// The message text. May be empty while the composer is still being typed into.
    pub text: String,
    /// The conversation this text was typed into, when it was typed into one. A
    /// draft with no conversation sends as a NEW chat.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub folder_path: Option<String>,
    pub source: Source,
    pub state: State,
    pub trigger: Trigger,
    /// Set when the last send attempt failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Latched by the dispatcher once this node has been observed with work
    /// running since the draft was armed. Only [`Trigger::AllDone`] consults it —
    /// see that variant for why "all agents done" needs a memory of them having
    /// started, and cannot be a pure reading of the present.
    #[serde(default)]
    pub seen_busy: bool,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<i64>,
}

/// The presentation fields the sidebar's declarative `map` reads. Computed here
/// rather than in the manifest because a `sidebar_sections` spec can only project
/// existing keys — it has no expression language, so "first line, truncated" and
/// "what is this waiting for" have to arrive already rendered.
#[derive(Debug, Clone, Serialize)]
pub struct DraftRow {
    #[serde(flatten)]
    pub draft: Draft,
    pub preview: String,
    pub state_label: &'static str,
    pub waiting_for: String,
}

/// How long a preview may be before it is cut. One sidebar row, one line.
const PREVIEW_CHARS: usize = 80;

impl From<Draft> for DraftRow {
    fn from(draft: Draft) -> DraftRow {
        let first_line = draft.text.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
        let trimmed = first_line.trim();
        // Truncate on a CHAR boundary: `text` is arbitrary user input and slicing a
        // multi-byte grapheme by byte index panics.
        let preview = if trimmed.chars().count() > PREVIEW_CHARS {
            let cut: String = trimmed.chars().take(PREVIEW_CHARS).collect();
            format!("{cut}…")
        } else {
            trimmed.to_owned()
        };
        let waiting_for = match draft.state {
            // An AllDone draft that has not seen the node busy yet would otherwise
            // read "waiting for every agent to finish" while nothing has ever
            // started — which looks like a stuck queue rather than a correct wait.
            State::Armed
                if draft.trigger == Trigger::AllDone && !draft.seen_busy =>
            {
                "Waiting for agents to start, then finish".to_owned()
            }
            State::Armed => draft.trigger.waiting_for(),
            State::Failed => draft.error.clone().unwrap_or_else(|| "Send failed".to_owned()),
            _ => String::new(),
        };
        DraftRow {
            preview,
            state_label: draft.state.label(),
            waiting_for,
            draft,
        }
    }
}

/// Node-wide behaviour. One row, id `default`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    /// Keep composer text the user walks away from.
    pub autosave_enabled: bool,
    /// Below this many characters, abandoned text is discarded rather than kept —
    /// a stray keystroke is not a draft.
    pub autosave_min_chars: u32,
    /// Turn a send into a queued draft when the node is already at `max_concurrent`.
    pub auto_queue_enabled: bool,
    /// The concurrency ceiling `auto_queue_enabled` compares against, and the
    /// `below` an auto-queued draft is armed with.
    pub max_concurrent: u32,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            autosave_enabled: true,
            autosave_min_chars: 4,
            // OFF by default: this one changes what happens when you press Enter,
            // and a user who has not asked for a queue should get the send they
            // pressed for. Autosave above is on because it only ever preserves
            // text that would otherwise be destroyed.
            auto_queue_enabled: false,
            max_concurrent: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn readings(running: Option<u32>, usage: &[(&str, f64)], now_ms: i64) -> Readings {
        Readings {
            running,
            usage: usage.iter().map(|(a, b)| ((*a).to_owned(), *b)).collect(),
            now_ms,
        }
    }

    #[test]
    fn manual_never_fires() {
        let r = readings(Some(0), &[("claude", 0.0)], i64::MAX);
        assert!(!Trigger::Manual.is_satisfied(&r, true));
    }

    #[test]
    fn all_done_does_not_fire_on_an_idle_node_that_was_never_busy() {
        // THE trap this variant exists for. `Concurrency { below: 1 }` fires here —
        // nothing is running — and that would send a "now that everything is
        // finished" prompt before any of the work had started.
        let idle = readings(Some(0), &[], 0);
        assert!(!Trigger::AllDone.is_satisfied(&idle, false));
        assert!(Trigger::Concurrency { below: 1 }.is_satisfied(&idle, false));
    }

    #[test]
    fn all_done_fires_only_after_busy_then_quiet() {
        assert!(!Trigger::AllDone.is_satisfied(&readings(Some(3), &[], 0), true));
        assert!(Trigger::AllDone.is_satisfied(&readings(Some(0), &[], 0), true));
    }

    #[test]
    fn all_done_holds_when_the_run_count_is_unknown() {
        // Fail-closed like every other reading: "we could not see the node" is not
        // "the node is quiet".
        assert!(!Trigger::AllDone.is_satisfied(&readings(None, &[], 0), true));
    }

    #[test]
    fn concurrency_fires_strictly_below_the_ceiling() {
        let t = Trigger::Concurrency { below: 3 };
        assert!(t.is_satisfied(&readings(Some(2), &[], 0), false));
        assert!(!t.is_satisfied(&readings(Some(3), &[], 0), false));
        assert!(!t.is_satisfied(&readings(Some(9), &[], 0), false));
    }

    #[test]
    fn an_unknown_reading_holds_the_draft() {
        // Fail-closed is the whole contract: no reading, no send.
        assert!(!Trigger::Concurrency { below: 3 }.is_satisfied(&readings(None, &[], 0), false));
        assert!(!Trigger::UsageReset {
            agent_id: "claude".into(),
            below_percent: 90.0,
        }
        .is_satisfied(&readings(Some(0), &[("codex", 1.0)], 0), false));
    }

    #[test]
    fn usage_fires_when_that_agents_window_has_room_again() {
        let t = Trigger::UsageReset {
            agent_id: "claude".into(),
            below_percent: 90.0,
        };
        assert!(!t.is_satisfied(&readings(Some(0), &[("claude", 99.5)], 0), false));
        assert!(t.is_satisfied(&readings(Some(0), &[("claude", 12.0)], 0), false));
    }

    #[test]
    fn a_time_trigger_fires_at_the_instant_not_after_it() {
        let t = Trigger::At { epoch_ms: 1_000 };
        assert!(!t.is_satisfied(&readings(None, &[], 999), false));
        assert!(t.is_satisfied(&readings(None, &[], 1_000), false));
    }

    #[test]
    fn preview_cuts_on_a_char_boundary() {
        let text = "é".repeat(200);
        let row = DraftRow::from(draft_with(text));
        // The point is that this does not panic; the ellipsis proves it truncated.
        assert!(row.preview.ends_with('…'));
        assert_eq!(row.preview.chars().count(), PREVIEW_CHARS + 1);
    }

    #[test]
    fn preview_skips_leading_blank_lines() {
        let row = DraftRow::from(draft_with("\n\n  the real first line\nsecond".to_owned()));
        assert_eq!(row.preview, "the real first line");
    }

    fn draft_with(text: String) -> Draft {
        Draft {
            id: "d1".into(),
            text,
            conversation_id: None,
            agent_id: None,
            model: None,
            folder_path: None,
            source: Source::Manual,
            state: State::Draft,
            trigger: Trigger::Manual,
            error: None,
            seen_busy: false,
            created_at: 0,
            updated_at: 0,
            claimed_at: None,
            sent_at: None,
        }
    }
}
