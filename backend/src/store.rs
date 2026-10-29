//! SQLite persistence for the outbox: `<plugin data dir>/drafts.db`
//! (`<data dir>/plugin-data/@ryu+drafts/drafts.db`).
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

use ryu_crypto::content_sqlite::{ColumnPlan, KeyKind};

use crate::models::{Draft, Readings, Settings, Source, State, Trigger};
use crate::seal_keys::PluginKeys;
use crate::seal_sql::{collect_readable, open_store, opened, opened_opt, SqlSeal};

/// How long a `sending` claim is honoured before another dispatcher may take the
/// draft. A dispatcher that crashes mid-send must not strand the draft forever, and
/// two minutes is far longer than any send takes while still being a delay a person
/// would sit through rather than assume the queue is broken.
pub const CLAIM_TIMEOUT_MS: i64 = 120_000;

/// The reason a draft carries after [`Store::release_expired_claims`] fails it.
pub const INTERRUPTED_SEND_ERROR: &str = "Send was interrupted; review and resend";

/// Resolve this app's data directory: the `RYU_PLUGIN_DATA_DIR` Core injects at
/// spawn, else `<data dir>/plugin-data/@ryu+drafts` for a standalone run.
pub fn data_dir() -> PathBuf {
    ryu_sidecar_runtime::plugin_data_dir("@ryu/drafts")
}

pub struct Store {
    conn: Mutex<Connection>,
    seal: SqlSeal,
}

/// The plugin id (the sealing key's scope).
pub const PLUGIN_ID: &str = "@ryu/drafts";
/// The registered content-store name (part of every key derivation; never rename).
pub const STORE: &str = "drafts.db";

/// The draft columns, with the sealed prompt text and send error opened.
const DRAFT_COLUMNS: &str = "id, ryu_open('drafts.text', id, text) AS text, conversation_id, \
     agent_id, model, folder_path, source, state, trigger_json, \
     ryu_open('drafts.error', id, error) AS error, created_at, updated_at, claimed_at, sent_at, \
     seen_busy";

/// The sealed columns (`encb:v1:`, bound to `drafts.<column>` and the id).
pub fn column_plans() -> Result<Vec<ColumnPlan>> {
    ["text", "error"]
        .iter()
        .map(|column| ColumnPlan::text("drafts", column, "id", KeyKind::Text))
        .collect()
}

impl Store {
    /// Open `<dir>/drafts.db`, resolving this process's sealing key with `dir`
    /// as the plugin data dir (see [`crate::seal_keys`]).
    pub fn open(dir: impl AsRef<Path>) -> Result<Store> {
        let keys = PluginKeys::resolve(PLUGIN_ID, dir.as_ref())?;
        Store::open_with_keys(dir, &keys)
    }

    /// [`Self::open`] with explicit keys.
    pub fn open_with_keys(dir: impl AsRef<Path>, keys: &PluginKeys) -> Result<Store> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let path = dir.join(STORE);
        Store::from_connection(Connection::open(&path)?, keys, Some(&path))
    }

    /// In-memory store sealing under a random key. Tests only — every test gets
    /// its own database rather than sharing a scratch directory that a parallel
    /// run could clobber.
    #[cfg(test)]
    pub fn memory() -> Result<Store> {
        use base64::Engine as _;
        use rand::RngCore as _;
        let mut root = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut root);
        let keys = PluginKeys::sealing(
            ryu_crypto::PluginCipher::from_subkey_b64(
                PLUGIN_ID,
                &base64::engine::general_purpose::STANDARD.encode(root),
            )?,
            crate::seal_keys::KeySource::Delivered,
        );
        Store::from_connection(Connection::open_in_memory()?, &keys, None)
    }

    /// The sealing health item: `{state, detail?, unreadable_rows}`.
    pub fn seal_health(&self) -> serde_json::Value {
        self.seal.health()
    }

    fn from_connection(
        mut conn: Connection,
        keys: &PluginKeys,
        path: Option<&Path>,
    ) -> Result<Store> {
        // Before any statement that could rewrite a cell.
        ryu_crypto::content_sqlite::enable_secure_delete(&conn)?;
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
        let seal = open_store(&mut conn, keys, STORE, &column_plans()?, path)
            .context("sealing drafts.db")?;
        Ok(Store {
            conn: Mutex::new(conn),
            seal,
        })
    }

    /// Every draft the sidebar should show: everything except the ones already
    /// sent, newest first. `sending` is included so a draft does not blink out of
    /// the list for the second or two a dispatcher holds it.
    pub fn list_open(&self) -> Result<Vec<Draft>> {
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            &format!("SELECT {DRAFT_COLUMNS} FROM drafts WHERE state != 'sent' ORDER BY updated_at DESC LIMIT 500"),
        )?;
        let rows = stmt.query_map([], row_to_draft)?;
        Ok(collect_readable(rows)?)
    }

    /// Recently sent drafts, newest first — the history half of the companion.
    pub fn list_sent(&self, limit: u32) -> Result<Vec<Draft>> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(&format!("SELECT {DRAFT_COLUMNS} FROM drafts WHERE state = 'sent' ORDER BY sent_at DESC LIMIT ?1"))?;
        let rows = stmt.query_map(params![limit], row_to_draft)?;
        Ok(collect_readable(rows)?)
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
        let mut stmt = conn.prepare(&format!(
            "SELECT {DRAFT_COLUMNS} FROM drafts WHERE state = 'armed'"
        ))?;
        let rows = stmt.query_map([], row_to_draft)?;
        Ok(collect_readable(rows)?)
    }

    pub fn get(&self, id: &str) -> Result<Option<Draft>> {
        let conn = self.lock()?;
        Ok(conn
            .query_row(
                &format!("SELECT {DRAFT_COLUMNS} FROM drafts WHERE id = ?1"),
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
        // A draft that is mid-send must not be rewritten under the dispatcher. An
        // expired claim belongs to a dispatcher that died, so it is editable.
        if existing.as_ref().is_some_and(|e| claim_is_live(e, now)) {
            return Err(anyhow!("draft {} is being sent", draft.id));
        }
        draft.created_at = existing.as_ref().map_or(now, |e| e.created_at);
        draft.updated_at = now;
        // Editing a failed draft clears the failure: the text the error was about
        // no longer exists. An abandoned send is treated the same way.
        if existing
            .as_ref()
            .is_some_and(|e| matches!(e.state, State::Failed | State::Sending))
        {
            draft.state = State::Draft;
            draft.error = None;
        }
        let conn = self.lock()?;
        conn.execute(
            "INSERT INTO drafts
               (id, text, conversation_id, agent_id, model, folder_path, source, state,
                trigger_json, error, created_at, updated_at, claimed_at, sent_at)
             VALUES (?1, ryu_seal('drafts.text', ?1, ?2), ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                     ryu_seal('drafts.error', ?1, ?10), ?11, ?12, ?13, ?14)
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
        if claim_is_live(&existing, now_ms()) {
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
                    seen_busy = 0, claimed_at = NULL
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
            "UPDATE drafts SET state = 'failed', error = ryu_seal('drafts.error', ?1, ?2),
                    updated_at = ?3, claimed_at = NULL
             WHERE id = ?1",
            params![id, error, now],
        )?;
        drop(conn);
        self.get(id)
    }

    /// Fail every draft whose `sending` claim has expired: its dispatcher died
    /// mid-send. Failed rather than re-armed, because Core may already have
    /// accepted the turn before the process went away, and re-arming could send
    /// the prompt twice. Returns how many drafts were released.
    pub fn release_expired_claims(&self) -> Result<usize> {
        let now = now_ms();
        let conn = self.lock()?;
        Ok(conn.execute(
            "UPDATE drafts
             SET state = 'failed', error = ?1, claimed_at = NULL, updated_at = ?2
             WHERE state = 'sending' AND (claimed_at IS NULL OR claimed_at < ?3)",
            params![INTERRUPTED_SEND_ERROR, now, now - CLAIM_TIMEOUT_MS],
        )?)
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

/// Whether `draft` is held by a dispatcher that may still be sending it.
fn claim_is_live(draft: &Draft, now: i64) -> bool {
    draft.state == State::Sending
        && draft
            .claimed_at
            .is_some_and(|claimed_at| claimed_at >= now - CLAIM_TIMEOUT_MS)
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn row_to_draft(row: &Row<'_>) -> rusqlite::Result<Draft> {
    let trigger_json: String = row.get("trigger_json")?;
    Ok(Draft {
        id: row.get("id")?,
        text: opened(row, "text")?,
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
        error: opened_opt(row, "error")?,
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
    use std::sync::{Arc, Barrier};

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
    fn drafts_and_settings_survive_store_relaunch() {
        let dir =
            std::env::temp_dir().join(format!("ryu-drafts-relaunch-{}", uuid::Uuid::new_v4()));
        {
            let store = Store::open(&dir).unwrap();
            store
                .upsert(new_draft("durable", "survive restart"))
                .unwrap();
            store
                .set_trigger("durable", Trigger::Concurrency { below: 4 })
                .unwrap();
            store
                .save_settings(&Settings {
                    autosave_enabled: false,
                    autosave_min_chars: 8,
                    auto_queue_enabled: true,
                    max_concurrent: 4,
                })
                .unwrap();
        }
        let reopened = Store::open(&dir).unwrap();
        let draft = reopened.get("durable").unwrap().unwrap();
        assert_eq!(draft.text, "survive restart");
        assert_eq!(draft.state, State::Armed);
        assert_eq!(draft.trigger, Trigger::Concurrency { below: 4 });
        let settings = reopened.settings().unwrap();
        assert!(!settings.autosave_enabled);
        assert!(settings.auto_queue_enabled);
        assert_eq!(settings.max_concurrent, 4);
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
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
    fn concurrent_claimers_have_exactly_one_winner() {
        let store = Arc::new(Store::memory().unwrap());
        store.upsert(new_draft("race", "send once")).unwrap();
        store
            .set_trigger("race", Trigger::Concurrency { below: 2 })
            .unwrap();
        let barrier = Arc::new(Barrier::new(9));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                store.claim("race").unwrap().is_some()
            }));
        }
        barrier.wait();
        let winners = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1, "the atomic claim must select one sender");
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
    fn an_abandoned_send_is_failed_and_editable_again() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "go")).unwrap();
        s.upsert(new_draft("live", "still sending")).unwrap();
        s.set_trigger("d1", Trigger::Concurrency { below: 9 })
            .unwrap();
        s.claim("d1").unwrap().unwrap();
        s.claim("live").unwrap().unwrap();
        // The dispatcher holding `d1` died before it could record the outcome.
        {
            let conn = s.lock().unwrap();
            conn.execute(
                "UPDATE drafts SET claimed_at = ?1 WHERE id = 'd1'",
                params![now_ms() - CLAIM_TIMEOUT_MS - 1],
            )
            .unwrap();
        }

        assert_eq!(s.release_expired_claims().unwrap(), 1);
        let released = s.get("d1").unwrap().unwrap();
        assert_eq!(released.state, State::Failed);
        assert_eq!(released.error.as_deref(), Some(INTERRUPTED_SEND_ERROR));
        assert_eq!(released.claimed_at, None);
        assert_eq!(s.get("live").unwrap().unwrap().state, State::Sending);

        let edited = s.upsert(new_draft("d1", "go again")).unwrap().unwrap();
        assert_eq!(edited.state, State::Draft);
        assert_eq!(edited.error, None);
    }

    #[test]
    fn an_expired_claim_does_not_block_editing_or_rearming() {
        let s = Store::memory().unwrap();
        s.upsert(new_draft("d1", "go")).unwrap();
        s.claim("d1").unwrap().unwrap();
        {
            let conn = s.lock().unwrap();
            conn.execute(
                "UPDATE drafts SET claimed_at = ?1 WHERE id = 'd1'",
                params![now_ms() - CLAIM_TIMEOUT_MS - 1],
            )
            .unwrap();
        }
        let armed = s
            .set_trigger("d1", Trigger::Concurrency { below: 9 })
            .unwrap()
            .unwrap();
        assert_eq!(armed.state, State::Armed);
        assert_eq!(armed.claimed_at, None);
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

    mod at_rest {
        use super::*;
        use crate::seal_keys::KeySource;
        use base64::Engine as _;
        use ryu_crypto::{ContentCipher, PluginCipher};

        const SECRET: &str = "zebra-prompt-marker";
        const FAILURE: &str = "quokka-failure-marker";

        fn keys(n: u8) -> PluginKeys {
            PluginKeys::sealing(
                PluginCipher::from_subkey_b64(
                    PLUGIN_ID,
                    &base64::engine::general_purpose::STANDARD.encode([n; 32]),
                )
                .unwrap(),
                KeySource::Delivered,
            )
        }

        fn dir() -> PathBuf {
            std::env::temp_dir().join(format!("ryu-drafts-at-rest-{}", uuid::Uuid::new_v4()))
        }

        fn on_disk(dir: &Path, needle: &str) -> bool {
            let mut bytes = std::fs::read(dir.join(STORE)).unwrap_or_default();
            bytes.extend(std::fs::read(dir.join(format!("{STORE}-wal"))).unwrap_or_default());
            bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
        }

        fn seed(store: &Store) {
            store.upsert(new_draft("d1", SECRET)).unwrap();
            store.upsert(new_draft("d2", "second draft")).unwrap();
            store.mark_failed("d1", FAILURE).unwrap();
        }

        fn raw(dir: &Path) -> Vec<String> {
            let conn = Connection::open(dir.join(STORE)).unwrap();
            let mut stmt = conn
                .prepare("SELECT text || '|' || COALESCE(error, '') FROM drafts ORDER BY id")
                .unwrap();
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            rows
        }

        #[test]
        fn prompts_and_errors_are_sealed_on_disk_and_read_back() {
            let dir = dir();
            let store = Store::open_with_keys(&dir, &keys(1)).unwrap();
            seed(&store);
            let got = store.get("d1").unwrap().unwrap();
            assert_eq!(got.text, SECRET);
            assert_eq!(got.error.as_deref(), Some(FAILURE));
            assert_eq!(store.list_open().unwrap().len(), 2);
            drop(store);
            assert!(!on_disk(&dir, SECRET));
            assert!(!on_disk(&dir, FAILURE));
            let conn = Connection::open(dir.join(STORE)).unwrap();
            let cipher = keys(1).content_cipher(STORE).unwrap();
            ryu_crypto::content_sqlite::assert_all_open(&conn, &cipher, &column_plans().unwrap())
                .unwrap();
        }

        #[test]
        fn legacy_drafts_are_sealed_once_and_still_read() {
            let dir = dir();
            seed(&Store::open_with_keys(&dir, &PluginKeys::compat()).unwrap());
            assert!(on_disk(&dir, SECRET));
            let store = Store::open_with_keys(&dir, &keys(2)).unwrap();
            assert_eq!(store.get("d1").unwrap().unwrap().text, SECRET);
            drop(store);
            assert!(!on_disk(&dir, SECRET));
            assert!(!on_disk(&dir, FAILURE));
            let first = raw(&dir);
            assert!(first
                .iter()
                .all(|row| ContentCipher::is_sealed_text(row.split('|').next().unwrap())));
            drop(Store::open_with_keys(&dir, &keys(2)).unwrap());
            assert_eq!(raw(&dir), first);
        }

        #[test]
        fn a_copied_prompt_is_skipped_in_lists_and_other_keys_refuse_writes() {
            let dir = dir();
            let store = Store::open_with_keys(&dir, &keys(3)).unwrap();
            seed(&store);
            store
                .lock()
                .unwrap()
                .execute(
                    "UPDATE drafts SET text = (SELECT text FROM drafts WHERE id = 'd1') WHERE id = 'd2'",
                    [],
                )
                .unwrap();
            assert_eq!(store.list_open().unwrap().len(), 1);
            assert!(store.get("d2").is_err());
            assert!(store.seal_health()["unreadable_rows"].as_u64().unwrap() >= 1);
            drop(store);
            for other in [keys(4), PluginKeys::compat()] {
                let wrong = Store::open_with_keys(&dir, &other).unwrap();
                assert_eq!(wrong.seal_health()["state"], "unhealthy");
                assert!(wrong.upsert(new_draft("d3", "refused")).is_err());
            }
        }
    }
}
