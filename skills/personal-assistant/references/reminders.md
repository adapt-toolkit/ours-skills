# Due-only reminders

For destination-free agent self-wakes, use the `self-reminders` skill with native Fleet tools.
New timers use `fleet_reminder_create`; due `[fleet-reminder]` events use native
inspect/ack. No custom worker setup is needed.
This reference documents the legacy personal source/delivery ledger. No route is
required for its create/list/update/cancel/inspect/serve operations; the verified
route and Fleet CLI are required only when explicitly delivering outward.
`complete` with `event_id` and `expected_revision` acknowledges an internal wake
without sending; `handled_internal` never means user delivery. Existing configured
outbound reminder semantics and ledgers remain intact.

Use `python3 scripts/reminder_tools.py --config <private-host.json> ACTION`.
Pass one JSON object on stdin, except `list` and the deterministic `serve` worker.
No model is called to check an empty reminder store. All dates below are synthetic.

- `create`: `title`, `local_time` (full ISO date/time without UTC offset), `timezone`
  (IANA), optional `fold`, `context`, `source_ref`, `late_window_seconds` (default
  3600, maximum seven days), `mode` (`fixed` default or `contextual`),
  `followup_limit` (integer0..7, default0), optional `followup_until_utc` (explicit
  UTC ISO ending Z, required when budget>0), `followup_min_seconds` (60..604800,
  default3600). Positive follow-up budget requires contextual mode and an agreed
  deadline after this due instant. Returns `id`, `revision`, resolved UTC epoch `due`/`expires`.
- `update`: `id`, `expected_revision`, changed create fields. Recreates a revision;
  the old queued event becomes ineligible. Follow-up permission may only tighten;
  terminal confirmed/dismissed reminders cannot be reopened. Follow-up child changes
  retain inherited purpose/source/mode/zone, current source sendability/lineage,
  minimum spacing and deadline-capped expiry; changing purpose needs new explicit intake. Confirm affected date/time with the user.
- `snooze`: `id`, `expected_revision`, explicit new `local_time` and `timezone`,
  optional `fold` and `late_window_seconds`. Never infer the date from “later”.
- `cancel`: `id`, `expected_revision`. Cancels future eligibility, including events
  already queued while the assistant was busy. An already emitted message cannot be recalled.
- `list`: optional JSON `status` (active default), finite UTC epoch `due_from`/`due_to`, `limit`1..25 and previous `cursor`. Returns bounded allowlisted summaries, truthful `returned`/`more` and last-emitted cursor. Filter/generation changes require refresh; no automatic paging.
- `inspect`: `event_id`. Returns eligibility, current reminder and admission/delivery ledger.
  Contextual CLI inspection adds `recovered_context` for a configured source reference
  (or the stored generic-goal fingerprint). An actual admitted prompt receipt owns a persisted budget; unavailable/invalid context holds, permanently stopped or call-exhausted tickets emit no further payload. Never reset the ticket or recover through another CLI.
- `deliver`: only `event_id`. Sends the current reminder title/context through the
  running assistant's supported supervisor `send_message` tool to the fixed verified
  host route. Caller cannot provide a contact, Telegram chat, or replacement body.
- `deliver_contextual`: `event_id`, integer `expected_revision`, `text` (nonblank,
  no NUL; no artificial local length margin), SHA256 `source_fingerprint`. Only contextual events;
  rechecks source snapshot/current revision/relevance and durable prior claim before
  the same pinned send. The ledger records the outgoing body hash, not raw generated text.
- `dismiss_contextual`: `event_id`, integer `expected_revision`, `reason`
  (`no_update`, `not_relevant`, `context_missing`), current `source_fingerprint`.
  Readable decisions are terminal with no send. Genuine missing-source context is held
  unsent until repaired/expired rather than fabricating a fingerprint or blind retry.
- `create_followup`: `parent_event_id`, integer `expected_revision`, explicit
  `local_time`, same confirmed `timezone`, optional `fold`, `late_window_seconds`.
  Only a confirmed contextual parent or `no_update` dismissal within inherited
  finite budget/deadline/spacing can make one child. Same request is idempotent;
  conflicting retry rejects. No caller title/purpose/source/mode/route override.
- `serve`: waits deterministically and submits contextual events using installed
  Fleet `controlRequest(... submit_prompt ...)`, the same local authenticated surface
  as `ours-fleet send`. It does not fabricate owner-admin console provenance.

For indexed source-bound create/update/cancel/snooze, include an immutable `operation_id` and exact `expected_source_revision`; ordinary updates/cancel/snooze also need reminder `expected_revision`. The configured journal freezes source, persists one ledger receipt/due hold, maps that exact receipt, then releases the due hold. Conflicts remain held for explicit reconciliation. Source-bound contextual follow-ups use a runtime-derived parent operation ID and inherit/map their exact source. See [storage](storage.md).

For preparation reminders, subtract confirmed effort plus agreed buffer from the
event start **in UTC**, then convert that instant back to the event's IANA zone
and use the corresponding local time and fold. This handles a DST transition
between preparation and event. Use `source_ref` to link the source event or task.
Task prerequisites belong in the separate source store. An unlock result is
context for the current owner turn; schedule a follow-up only within the already requested/agreed bounded follow-up permission. See [contextual behavior](contextual.md).

## Runtime contract and limits

The worker checks only the local database/clock at most every 30 seconds and waits
to the earliest known due instant. Creating a reminder while the worker sleeps can
add up to 30 seconds admission delay. This is bounded best-effort timing, not an
exact-time guarantee. A busy assistant accepts the event behind its existing turn;
its actual response can be later. Before emitting, `deliver` checks revision,
cancellation and the agreed relevance deadline. Host sleep, powered-off periods,
network loss and disabled user services prevent delivery; boot/resume catches up
only reminders still relevant. Clock changes are noticed on the next deterministic
check. UTC resolution is persisted, so later tzdata changes do not silently shift
an existing appointment; explicitly update if the user's intended civil time changes.

SQLite FULL durability and transactions retain source, event admission and send
ledger. A per-database flock rejects a second worker. The previous native model
loop must remain disabled. Runtime activation must preserve the existing source
state and current admission/send/dedup ledger; new users use a separate empty ledger, while existing users retain/migrate their reviewed current ledger. It does not import hypothetical
examples from an existing personal state file.

`queued` means Fleet accepted an event, not that an LLM turn completed or a message
arrived. Only definite pre-admission unavailability retries (30s then 60s, total
three attempts per event); rejection stops. Unknown admission and process interruption
are `uncertain`, never blind-replayed. The assistant can process the original uncertain
event if it actually arrives, but its delivery claim is still single-attempt.

`transport_confirmed` requires `send_message.sent=true` and a wire ID. It proves
the Ours send result only. For Telegram it **does not prove Telegram API acceptance
or Owner receipt**. The existing connector retries independently and documents an
external accept-before-daemon-ack crash window that may duplicate a Telegram message.
Do not call this end-to-end exactly-once delivery.

An uncertain send remains blocked from another send or rescheduling until a local
operator checks the exact event, prompt receipt, outgoing wire metadata and (where
needed) Owner receipt. There is intentionally no “retry uncertain” assistant tool.
Retain the ledger; do not delete a row to trigger another send. If transport is
definitely rejected with no side effect, explicitly update/snooze to a new confirmed
date/time. If evidence is unavailable, disclose uncertainty and keep it held.

## Host config (private, never include in shareable skill)

Copy the parameterized config template, make it mode 0600 in a private user state
directory, and set database path, permanent agent name/state path, installed Fleet
control module path. Configure the Fleet CLI and a **verified** outbound route CID only when explicit legacy outbound delivery is needed. `source_db` is this
user's separate private SQLite source-store path with source_schema2 and preserved source_namespace for indexed recovery; source_journal is its private operations journal. For an existing legacy JSON source use mutually exclusive `source_json` plus a stable configured canonical UUID `source_namespace`; preserve the JSON writer/state/preferences and use its reviewed read-only event adapter. Undefined JSON task schema is held. Never configure both backends or point SQLite at JSON;
generic contextual goals use only their current stored context fingerprint. Telegram additionally
requires a pinned positive private chat ID equal to the verified Owner Telegram
user ID, bot name and route identity proof. Never use catch-all chat 0 or a group.
Use Ours until the Telegram route is actually proven; switch one outbound route,
never emit the same reminder on both channels automatically.

The systemd template runs only the deterministic worker. Configure a unique per-user
unit, a private environment/config path, and known installed Python/Fleet paths.
Enable through the task's supported management handoff after mutual review; do not
restart the assistant or shared connector. User linger is needed for boot without
login. Do not create cron/systemd jobs that periodically prompt the model.

## Scheduling research

| Mechanism | Decision |
|---|---|
| Existing Fleet periodic loops | Omit for reminders: Owner rejects periodic model cost; busy/missed-occurrence semantics do not preserve every appointment. Installed Fleet README and [upstream source](https://github.com/adapt-toolkit/ours-fleet) checked. |
| cron | Omit: minute granularity and a separate durable queue/relevance ledger still needed. A cron job invoking the model periodically violates the Owner requirement. |
| systemd calendar timers | Valid deterministic option with Persistent catch-up and accuracy controls, but per-reminder timer-unit mutation adds state to reconcile with CRUD. [Primary systemd timer manual source](https://github.com/systemd/systemd/blob/main/man/systemd.timer.xml). |
| One SQLite worker managed by systemd | Chosen: reminders and revisions have one transactional source of truth; zero empty model turns; one admission per actual due event unless definitely unavailable, with an explicit uncertainty hold. |


## Reviewed contextual upgrades

Candidate verification uses synthetic stores only. The additive migration preserves legacy
rows as fixed mode with follow-up budget zero, adding contextual fields and a durable
unique parent-to-child table. The supervisor must coordinate one stopped helper and
quiescent assistant reminder mutations before replacing reviewed code/config and migrating
an actual permanent database. Do not test an unreviewed candidate against live state.

The old v1 script uses positional inserts and cannot safely write the extended reminder
schema. After migration, preserve the v2 schema/ledger and use reviewed v2 fixed mode as
the fallback; do not blindly restore v1 code or an old database snapshot after new events
or sends. Any schema rollback needs an operator-reviewed plan that retains all intervening
source and delivery records. No upgrade authorizes replay of already confirmed events.
