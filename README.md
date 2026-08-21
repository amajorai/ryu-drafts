<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./icon-dark.png" />
    <img src="./icon-light.png" alt="Drafts" width="144" />
  </picture>
</p>

<div align="center">

# Drafts

</div>

A durable outbox for unsent messages: the draft store, the trigger vocabulary that decides when a draft becomes sendable, and the claim protocol that keeps two desktop windows from sending the same draft twice, all held out-of-process in a SQLite-backed sidecar Core proxies to on loopback.

> **The public home of `ryu-drafts`.** Source, builds, and releases live here —
> binaries for every platform are attached to each release.
>
> This tree is generated from the Ryu monorepo, so commits pushed here
> directly are replaced on the next sync. **Pull requests are welcome** —
> open them here and they are ported into the monorepo, then flow back out.
> Ryu as a whole: https://github.com/amajorai/ryu

## Install

**App:** [Install](ryu://apps/@ryu/drafts) (opens the Ryu desktop app and asks you to confirm)

**CLI:**

```bash
ryu apps add @ryu/drafts
```

**Crate:**

```bash
cargo install ryu-drafts
```

Prebuilt binaries for every platform are attached to [each release](https://github.com/amajorai/ryu/releases).

## License

Apache-2.0 — see [LICENSE](./LICENSE).

## What it does

- **Keeps what you didn't send.** Type into a composer, leave, and the text is in the
  Drafts section of the sidebar instead of gone. One draft per conversation, edited in
  place — not one row per typing pause. Clearing the composer deletes it.
- **Queues work you can't start yet.** Arm a draft with "send when fewer than N agents
  are running" and it goes out the moment a slot frees, oldest first.
- **Waits out a usage cap.** Arm a draft with "send when this agent's usage window has
  room again" and a prompt written against a capped subscription goes out when the cap
  lifts, instead of failing on send.
- **Follows a batch of work.** Arm a draft with "send when everything finishes" and it
  goes out once the node has been busy and then goes quiet — the release prompt that
  should run after your agents are done.

## The conditions, and why so few

A queue that silently never drains is worse than no queue, so a condition only exists
here if something can actually observe it becoming true:

| Condition | Read from |
|---|---|
| `all_done` — the node was busy, and is now quiet | `node.readings` |
| `concurrency` — fewer than N runs active | `node.readings` |
| `usage_reset` — that agent's fullest window is back under X% | `node.readings` |
| `at` — a wall-clock instant | the clock |

`all_done` is **not** `concurrency { below: 1 }`, and the difference is the reason it
exists. `below: 1` is a statement about the present: arm it on an idle node and it fires
on the next tick, because nothing is running *right now*. Queue "publish the release now
that the agents are done" while you are still setting the work up, and it would go out
before any of it ran.

So `all_done` is a statement about a **transition** — the node must be seen busy, and
then seen quiet. Each armed draft carries its own `seen_busy` latch, set by the
dispatcher on a tick that observes work running (and reset every time the draft is
re-armed, so it never inherits an observation from before you changed your mind). The
consequence is deliberate: armed on a node where nothing ever starts, it waits forever
rather than firing. It was queued to follow some work, and there has been no work to
follow — the row says "Waiting for agents to start, then finish" so that reads as a
correct wait rather than a stuck queue.

`usage_reset` is expressed as "has room again" rather than "the window reset at T"
because a usage window's `resets_at` is optional (vendors report it inconsistently),
while `used_percent` is always present. A reset shows up as the percentage falling, so
the trigger fires on the observable fact rather than on a promised time that may never
arrive.

Every reading is **fail-closed**. An unknown fact holds the draft: if the dispatcher
could not read the node this tick, nothing is sent. The cost of holding is that a
message goes out later than it could; the cost of firing on an unknown is a message
sent into a node that is still capped, which is the failure this app exists to prevent.

## How it is put together

```
ryu-drafts (sidecar, :8012)            desktop shell
  the draft store (SQLite)               useComposerDraftAutosave — mirrors unsent
  the trigger vocabulary + latch           composer text into the store
  dispatch.rs — reads the node,          useComposerAutoQueue — turns a send into
    claims a ready draft, SENDS it         a queued draft when already at capacity
  GET /list → sidebar section            DraftsPage — edit, arm, delete
```

**The sidecar sends, with permission.** A manifest sidecar holds no `RYU_TOKEN` — Core
spawns it without one on purpose, since inheriting it would let a backend forge every
other plugin's ext-token. It does not need one: it calls two granted kernel capabilities
over loopback, `node.readings` (the live facts) and `chat.startTurn` (post the turn).
`chat.startTurn` is gated four ways — minted token, the `chat.sendFollowUp` grant, an
exec-firewall scan of the prompt, and by default the Approvals inbox, so you confirm
before a token is spent. That is what lets a draft go out at 04:00 with nothing open.

**"Sent" means handed off, not delivered.** With the approval gate on, a dispatched draft
is marked sent once the send belongs to the Approvals inbox — that is where you act on
it. Leaving it armed would re-queue the same message every tick and pile up duplicate
approvals. Nothing is lost either way: the draft keeps its full text in the Sent history.

**Two dispatchers cannot send the same draft twice.** Before sending, the dispatcher claims
the draft: a single `UPDATE ... WHERE state = 'armed'` whose affected-row count decides
the winner. The loser gets a 409 and moves on. A claim expires after two minutes, so a
dispatcher that dies mid-send releases the draft instead of stranding it.

## Settings

| Setting | Default | What it does |
|---|---|---|
| Keep unsent composer text | on | Autosave. Only ever preserves text that would otherwise be destroyed. |
| Queue sends when already busy | **off** | Turns a send into a queued draft at the concurrency ceiling. Off by default because it changes what pressing Enter does, and a user who has not asked for a queue should get the send they pressed for. |
| Concurrent runs allowed | 3 | The ceiling the above compares against, and the `below` an auto-queued draft is armed with. |

## Data

`$RYU_DIR/drafts/drafts.db`. Wiped by the app's `data_categories` entry ("Delete all
Drafts data"), which removes every draft, queue entry and send record. Messages already
sent stay in their conversations — this deletes the outbox, not the chats.
