//! SQLite persistence for the outbox: `$RYU_DIR/drafts/drafts.db`.
//!
//! A database rather than a file per draft (which is what `ryu-reasoning` does for
//! policies) because a draft is not a document an author diffs — it is a row in a
//! queue with a state machine, an ordering, and a CLAIM that two clients must not
//! win at the same time. [`Store::claim`] is a single `UPDATE ... WHERE state =
//! 'armed'` whose affected-row count decides the winner; that is the property the
//! whole design rests on, and it is exactly what a directory of JSON files cannot
//! give.
//!
//! Every instant is an INTEGER epoch-millis column. `trigger` is stored as its JSON
//! encoding so adding a trigger kind is a code change, not a migration.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::models::{Draft, Readings, Settings, Source, State, Trigger};

/// How long a `sending` claim is honoured before another dispatcher may take the
/// draft. A dispatcher that crashes mid-send must not strand the draft forever, and
/// two minutes is far longer than any send takes while still being a delay a person
/// would sit through rather than assume the queue is broken.
pub const CLAIM_TIMEOUT_MS: i64 = 120_000;

/// Resolve the data directory, honouring the `RYU_DIR` Core injects at spawn so the
/// sidecar writes under the same node directory Core uses.
pub fn data_dir() -> PathBuf {
    ryu_sidecar_runtime::ryu_dir().join("drafts")
}

pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn open(dir: impl AsRef<Path>) -> Result<Store> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        Store::from_connection(Connection::open(dir.join("drafts.db"))?)
    }

    /// In-memory store. Tests only — every test gets its own database rather than
    /// sharing a scratch directory that a parallel run could clobber.
    #[cfg(test)]
    pub fn memory() -> Result<Store> {
        Store::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(conn: Connection) -> Result<Store> {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS drafts (
               id              TEXT PRIMARY KEY,
               text            TEXT NOT NULL,
               conversation_id TEXT,
               agent_id        TEXT,
               model           TEXT,
               folder_path     TEXT,
               source          TEXT NOT NULL,
               state           TEXT NOT NULL,
               trigger_json    TEXT NOT NULL,
               error           TEXT,
               created_at      INTEGER NOT NULL,
               updated_at      INTEGER NOT NULL,
               claimed_at      INTEGER,
               sent_at         INTEGER,
               seen_busy       INTEGER NOT NULL DEFAULT 0
             );
             CREATE INDEX IF NOT EXISTS drafts_state_updated
               ON drafts (state, updated_at DESC);
             CREATE TABLE IF NOT EXISTS settings (
               id            TEXT PRIMARY KEY,
               settings_json TEXT NOT NULL
             );",
        )?;
        // Migration for outboxes created before `seen_busy` existed. SQLite has no
        // `ADD COLUMN IF NOT EXISTS`, and a duplicate-column error is the ONLY
        // expected failure here, so it is swallowed while anything else would still
        // surface on the next statement. Cheaper and clearer than a version table
        // for one additive column with a default.
        let _ = conn.execute(
            "ALTER TABLE drafts ADD COLUMN seen_busy INTEGER NOT NULL DEFAULT 0",
            [],
        );
        Ok(Store {
            conn: Mutex::new(conn),
        })
    }

    /// Every draft the sidebar should show: everything except the ones already
    /// sent, newest first. `sending` is included so a draft does not blink out of
    /// the list for the second or two a dispatcher holds it.
    pub fn list_open(&self) -> Result<Vec<Draft>> {
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT * FROM drafts WHERE state != 'sent' ORDER BY updated_at DESC LIMIT 500",
        )?;
        let rows = stmt.query_map([], row_to_draft)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Recently sent drafts, newest first — the history half of the companion.
    pub fn list_sent(&self, limit: u32) -> Result<Vec<Draft>> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare("SELECT * FROM drafts WHERE state = 'sent' ORDER BY sent_at DESC LIMIT ?1")?;
        let rows = stmt.query_map(params![limit], row_to_draft)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// The armed drafts whose condition holds against `r`, oldest first.
    ///
    /// Oldest-first is deliberate: the queue is FIFO, so a prompt written an hour
    /// ago goes before one written a minute ago when a slot frees for both.
    pub fn ready(&self, r: &Readings) -> Result<Vec<Draft>> {
        // Latch BEFORE evaluating, in the same pass over the same reading. An
        // `AllDone` draft needs to have seen the node busy, and a tick that observes
        // work running is exactly that observation — it also cannot fire this tick
        // (something is running), so latching first can never make a draft go early.
        if r.running.is_some_and(|running| running > 0) {
            self.latch_seen_busy()?;
        }
        let mut out: Vec<Draft> = self
            .armed()?
            .into_iter()
            .filter(|d| d.trigger.is_satisfied(r, d.seen_busy))
            .collect();
        out.sort_by_key(|d| d.created_at);
        Ok(out)
    }

    /// Record that the node has been seen busy, for every armed draft waiting on it.
    ///
    /// Scoped to `state = 'armed'` so a draft armed AFTER this moment does not
    /// inherit an observation that predates it — otherwise "send when my agents
    /// finish", queued during a lull, would fire on the tail of somebody else's
    /// earlier work.
    fn latch_seen_busy(&self) -> Result<()> {
        let conn = self.lock()?;
        conn.execute(
            "UPDATE drafts SET seen_busy = 1 WHERE state = 'armed' AND seen_busy = 0",
            [],
        )?;
        Ok(())
    }

    fn armed(&self) -> Result<Vec<Draft>> {
        let conn = self.lock()?;
        let mut stmt = conn.prepare("SELECT * FROM drafts WHERE state = 'armed'")?;
        let rows = stmt.query_map([], row_to_draft)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn get(&self, id: &str) -> Result<Option<Draft>> {
        let conn = self.lock()?;
        Ok(conn
            .query_row(
                "SELECT * FROM drafts WHERE id = ?1",
                params![id],
                row_to_draft,
            )
            .optional()?)
    }

    /// Insert a new draft, or replace the text of an existing one.
    ///
    /// `id` is caller-supplied so the composer can keep autosaving into the SAME
    /// draft as the user types instead of leaving a trail of one draft per
    /// keystroke pause. A blank-text upsert onto an existing draft DELETES it —
    /// that is the user clearing the composer, and keeping an empty row would put a
    /// permanent "Empty draft" in the sidebar.
    pub fn upsert(&self, mut draft: Draft) -> Result<Option<Draft>> {
        let now = now_ms();
        if draft.text.trim().is_empty() {
            if self.delete(&draft.id)? {
                return Ok(None);
            }
            return Ok(None);
        }
        let existing = self.get(&draft.id)?;
        // A draft that is mid-send must not be rewritten under the dispatcher.
        if existing.as_ref().is_some_and(|e| e.state == State::Sending) {
            return Err(anyhow!("draft {} is being sent", draft.id));
        }
        draft.created_at = existing.as_ref().map_or(now, |e| e.created_at);
        draft.updated_at = now;
        // Editing a failed draft clears the failure: the text the error was about
        // no longer exists.
        if existing.as_ref().is_some_and(|e| e.state == State::Failed) {
            draft.state = State::Draft;
            draft.error = None;
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO drafts
               (id, text, conversation_id, agent_id, model, folder_path, source, state,
                trigger_json, error, created_at, updated_at, claimed_at, sent_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
             ON CONFLICT(id) DO UPDATE SET
               text=excluded.text,
               conversation_id=excluded.conversation_id,
               agent_id=excluded.agent_id,
               model=excluded.model,
               folder_path=excluded.folder_path,
               state=excluded.state,
               trigger_json=excluded.trigger_json,
               error=excluded.error,
               updated_at=excluded.updated_at",
            params![
                draft.id,
                draft.text,
                draft.conversation_id,
                draft.agent_id,
                draft.model,
                draft.folder_path,
                draft.source.as_str(),
                draft.state.as_str(),
                serde_json::to_string(&draft.trigger)?,
                draft.error,
                draft.created_at,
                draft.updated_at,
                draft.claimed_at,
                draft.sent_at,
            ],
        )?;
        drop(conn);
        self.get(&draft.id)
    }

    /// Give a draft a condition. `Manual` disarms it back to an idle draft.
    pub fn set_trigger(&self, id: &str, trigger: Trigger) -> Result<Option<Draft>> {
        let Some(existing) = self.get(id)? else {
            return Ok(None);
        };
        if existing.state == State::Sending {
            return Err(anyhow!("draft {id} is being sent"));
        }
        let state = if trigger == Trigger::Manual {
            State::Draft
        } else {
            State::Armed
        };
        let conn = self.lock()?;
        conn.execute(
            "UPDATE drafts SET trigger_json = ?2, state = ?3, error = NULL, updated_at = ?4,
                    seen_busy = 0
             WHERE id = ?1",
            params![
                id,
                serde_json::to_string(&trigger)?,
                state.as_str(),
                now_ms()
            ],
        )?;
        drop(conn);
        self.get(id)
    }

    /// Take a draft for sending. Returns `None` when someone else already has it.
    ///
    /// The whole double-send guard is this one statement: the `WHERE` matches only a
    /// draft that is still armed (or whose claim has expired), so of two dispatchers
    /// racing on the same row exactly one gets `rows == 1`.
    pub fn claim(&self, id: &str) -> Result<Option<Draft>> {
        let now = now_ms();
        let conn = self.lock()?;
        let rows = conn.execute(
            "UPDATE drafts SET state = 'sending', claimed_at = ?2, updated_at = ?2
             WHERE id = ?1
               AND (state = 'armed'
                    OR state = 'draft'
                    OR (state = 'sending' AND claimed_at IS NOT NULL AND claimed_at < ?3))",
            params![id, now, now - CLAIM_TIMEOUT_MS],
        )?;
        drop(conn);
        if rows == 0 {
            return Ok(None);
        }
        self.get(id)
    }

    /// Record a successful send. The draft is kept as history, not deleted, so
    /// "what did I queue and did it go" has an answer.
    pub fn mark_sent(&self, id: &str, conversation_id: Option<&str>) -> Result<Option<Draft>> {
        let now = now_ms();
        let conn = self.lock()?;
        conn.execute(
            "UPDATE drafts
             SET state = 'sent', sent_at = ?2, updated_at = ?2, claimed_at = NULL, error = NULL,
                 conversation_id = COALESCE(?3, conversation_id)
             WHERE id = ?1",
            params![id, now, conversation_id],
        )?;
        drop(conn);
        self.get(id)
    }

    /// Record a failed send. The draft becomes visible again carrying the reason.
    pub fn mark_failed(&self, id: &str, error: &str) -> Result<Option<Draft>> {
        let now = now_ms();
        let conn = self.lock()?;
        conn.execute(
            "UPDATE drafts SET state = 'failed', error = ?2, updated_at = ?3, claimed_at = NULL
             WHERE id = ?1",
            params![id, error, now],
        )?;
        drop(conn);
        self.get(id)
    }

    pub fn delete(&self, id: &str) -> Result<bool> {
        let conn = self.lock()?;
        Ok(conn.execute("DELETE FROM drafts WHERE id = ?1", params![id])? > 0)
    }

    /// Delete everything. Backs the manifest's `data_categories` entry.
    pub fn delete_all(&self) -> Result<usize> {
        let conn = self.lock()?;
        Ok(conn.execute("DELETE FROM drafts", [])?)
    }

    pub fn settings(&self) -> Result<Settings> {
        let conn = self.lock()?;
        let raw: Option<String> = conn
            .query_row(
                "SELECT settings_json FROM settings WHERE id = 'default'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        // A settings row that no longer parses (an older shape, a hand-edit) must
        // not take the app down — the defaults are always a valid answer.
        Ok(raw
            .and_then(|r| serde_json::from_str(&r).ok())
            .unwrap_or_default())
    }

    pub fn save_settings(&self, settings: &Settings) -> Result<()> {
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO settings (id, settings_json) VALUES ('default', ?1)
             ON CONFLICT(id) DO UPDATE SET settings_json = excluded.settings_json",
            params![serde_json::to_string(settings)?],
        )?;
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| anyhow!("drafts store lock poisoned"))
    }
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn row_to_draft(row: &Row<'_>) -> rusqlite::Result<Draft> {
    let trigger_json: String = row.get("trigger_json")?;
    Ok(Draft {
        id: row.get("id")?,
        text: row.get("text")?,
        conversation_id: row.get("conversation_id")?,
        agent_id: row.get("agent_id")?,
        model: row.get("model")?,
        folder_path: row.get("folder_path")?,
        source: Source::parse(&row.get::<_, String>("source")?),
        state: State::parse(&row.get::<_, String>("state")?),
        // An unreadable trigger degrades to Manual rather than failing the row: the
        // draft's TEXT is the irreplaceable part, and losing a condition is
        // recoverable by re-arming it.
        trigger: serde_json::from_str(&trigger_json).unwrap_or(Trigger::Manual),
        error: row.get("error")?,
        created_at: row.get("created_at")?,
        updated_at: row.get("updated_at")?,
        seen_busy: row.get::<_, i64>("seen_busy").unwrap_or(0) != 0,
        claimed_at: row.get("claimed_at")?,
        sent_at: row.get("sent_at")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_draft(id: &str, text: &str) -> Draft {
        Draft {
            id: id.into(),
            text: text.into(),
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

    #[test]
    fn a_draft_survives_being_written_and_read_back() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "hello")).unwrap();
        let got = s.get("d1").unwrap().unwrap();
        assert_eq!(got.text, "hello");
        assert_eq!(got.state, State::Draft);
    }

    #[test]
    fn upserting_the_same_id_edits_rather_than_duplicates() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "first")).unwrap();
        let created = s.get("d1").unwrap().unwrap().created_at;
        s.upsert(new_draft("d1", "second")).unwrap();
        let after = s.get("d1").unwrap().unwrap();
        assert_eq!(after.text, "second");
        assert_eq!(after.created_at, created, "the creation stamp must survive");
        assert_eq!(s.list_open().unwrap().len(), 1);
    }

    #[test]
    fn clearing_the_text_deletes_the_draft() {
        // Otherwise every emptied composer leaves a permanent "Empty draft" row.
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "typed")).unwrap();
        s.upsert(new_draft("d1", "   ")).unwrap();
        assert!(s.get("d1").unwrap().is_none());
    }

    #[test]
    fn arming_moves_it_to_armed_and_disarming_moves_it_back() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "queued work")).unwrap();
        let armed = s
            .set_trigger("d1", Trigger::Concurrency { below: 2 })
            .unwrap()
            .unwrap();
        assert_eq!(armed.state, State::Armed);
        let idle = s.set_trigger("d1", Trigger::Manual).unwrap().unwrap();
        assert_eq!(idle.state, State::Draft);
    }

    #[test]
    fn only_one_claim_wins() {
        // The double-send guard. Two dispatchers, one draft, one winner.
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "go")).unwrap();
        s.set_trigger("d1", Trigger::Concurrency { below: 9 })
            .unwrap();
        assert!(s.claim("d1").unwrap().is_some());
        assert!(
            s.claim("d1").unwrap().is_none(),
            "a second dispatcher must not get the same draft"
        );
    }

    #[test]
    fn an_expired_claim_is_reclaimable() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "go")).unwrap();
        s.set_trigger("d1", Trigger::Concurrency { below: 9 })
            .unwrap();
        s.claim("d1").unwrap().unwrap();
        // Backdate the claim past the timeout: the dispatcher that took it died.
        {
            let conn = s.lock().unwrap();
            conn.execute(
                "UPDATE drafts SET claimed_at = ?1 WHERE id = 'd1'",
                params![now_ms() - CLAIM_TIMEOUT_MS - 1],
            )
            .unwrap();
        }
        assert!(
            s.claim("d1").unwrap().is_some(),
            "a dead dispatcher must not strand the draft forever"
        );
    }

    #[test]
    fn a_draft_being_sent_cannot_be_rewritten_underneath_the_dispatcher() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "go")).unwrap();
        s.claim("d1").unwrap().unwrap();
        assert!(s.upsert(new_draft("d1", "changed")).is_err());
        assert!(s.set_trigger("d1", Trigger::Manual).is_err());
    }

    #[test]
    fn ready_returns_only_satisfied_drafts_oldest_first() {
        let s = Store::memory().unwrap();
        for (id, below) in [("old", 3_u32), ("new", 3), ("never", 1)] {
            s.upsert(new_draft(id, "x")).unwrap();
            s.set_trigger(id, Trigger::Concurrency { below }).unwrap();
        }
        // Force a deterministic FIFO order rather than relying on clock resolution.
        {
            let conn = s.lock().unwrap();
            conn.execute("UPDATE drafts SET created_at = 1 WHERE id = 'old'", [])
                .unwrap();
            conn.execute("UPDATE drafts SET created_at = 2 WHERE id = 'new'", [])
                .unwrap();
        }
        let r = Readings {
            running: Some(2),
            usage: vec![],
            now_ms: 0,
        };
        let ids: Vec<String> = s.ready(&r).unwrap().into_iter().map(|d| d.id).collect();
        assert_eq!(ids, vec!["old", "new"], "FIFO, and 'never' must not appear");
    }

    #[test]
    fn all_done_waits_for_the_node_to_have_been_busy() {
        // The end-to-end shape of the trap: armed on a quiet node, it must NOT go
        // out; once a tick has seen work running, the next quiet tick releases it.
        let s = Store::memory().unwrap();
        s.upsert(new_draft("release", "cut the release")).unwrap();
        s.set_trigger("release", Trigger::AllDone).unwrap();

        let quiet = Readings {
            running: Some(0),
            usage: vec![],
            now_ms: 0,
        };
        assert!(
            s.ready(&quiet).unwrap().is_empty(),
            "an AllDone draft must not fire before any work has started"
        );

        // A tick that sees work running latches, and cannot itself release it.
        let busy = Readings {
            running: Some(2),
            usage: vec![],
            now_ms: 0,
        };
        assert!(s.ready(&busy).unwrap().is_empty());
        assert!(s.get("release").unwrap().unwrap().seen_busy);

        let ids: Vec<String> = s.ready(&quiet).unwrap().into_iter().map(|d| d.id).collect();
        assert_eq!(ids, vec!["release"], "busy then quiet must release it");
    }

    #[test]
    fn re_arming_resets_the_busy_latch() {
        // Otherwise a draft disarmed and re-armed would inherit an observation from
        // before the user changed their mind, and fire on the next quiet tick.
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "x")).unwrap();
        s.set_trigger("d1", Trigger::AllDone).unwrap();
        s.ready(&Readings {
            running: Some(1),
            usage: vec![],
            now_ms: 0,
        })
        .unwrap();
        assert!(s.get("d1").unwrap().unwrap().seen_busy);

        s.set_trigger("d1", Trigger::AllDone).unwrap();
        assert!(
            !s.get("d1").unwrap().unwrap().seen_busy,
            "arming must start the watch afresh"
        );
    }

    #[test]
    fn the_latch_only_reaches_drafts_that_are_already_armed() {
        // A draft armed AFTER the busy period must not inherit that observation.
        let s = Store::memory().unwrap();
        s.upsert(new_draft("early", "x")).unwrap();
        s.set_trigger("early", Trigger::AllDone).unwrap();
        s.ready(&Readings {
            running: Some(1),
            usage: vec![],
            now_ms: 0,
        })
        .unwrap();

        s.upsert(new_draft("late", "y")).unwrap();
        s.set_trigger("late", Trigger::AllDone).unwrap();
        assert!(s.get("early").unwrap().unwrap().seen_busy);
        assert!(!s.get("late").unwrap().unwrap().seen_busy);
    }

    #[test]
    fn a_sent_draft_leaves_the_sidebar_but_stays_as_history() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "go")).unwrap();
        s.claim("d1").unwrap();
        s.mark_sent("d1", Some("conv-7")).unwrap();
        assert!(s.list_open().unwrap().is_empty());
        let sent = s.list_sent(10).unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].conversation_id.as_deref(), Some("conv-7"));
    }

    #[test]
    fn a_failed_send_puts_the_draft_back_with_its_reason() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "go")).unwrap();
        s.claim("d1").unwrap();
        let failed = s.mark_failed("d1", "agent offline").unwrap().unwrap();
        assert_eq!(failed.state, State::Failed);
        assert_eq!(failed.error.as_deref(), Some("agent offline"));
        assert_eq!(s.list_open().unwrap().len(), 1);
    }

    #[test]
    fn editing_a_failed_draft_clears_the_failure() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "go")).unwrap();
        s.claim("d1").unwrap();
        s.mark_failed("d1", "boom").unwrap();
        let edited = s.upsert(new_draft("d1", "go, fixed")).unwrap().unwrap();
        assert_eq!(edited.state, State::Draft);
        assert!(edited.error.is_none());
    }

    #[test]
    fn settings_round_trip_and_default_when_absent() {
        let s = Store::memory().unwrap();
        assert!(s.settings().unwrap().autosave_enabled);
        assert!(!s.settings().unwrap().auto_queue_enabled);
        let mut settings = Settings::default();
        settings.auto_queue_enabled = true;
        settings.max_concurrent = 7;
        s.save_settings(&settings).unwrap();
        let back = s.settings().unwrap();
        assert!(back.auto_queue_enabled);
        assert_eq!(back.max_concurrent, 7);
    }

    #[test]
    fn a_corrupt_settings_row_falls_back_to_defaults() {
        let s = Store::memory().unwrap();
        {
            let conn = s.lock().unwrap();
            conn.execute(
                "INSERT INTO settings (id, settings_json) VALUES ('default', '{ not json')",
                [],
            )
            .unwrap();
        }
        assert_eq!(s.settings().unwrap().max_concurrent, 3);
    }
}
