# Periodic open-task review

Use this when the installation configures `task_review_db`. It keeps open source
tasks from being forgotten after finite reminders/returns are used up, without
asking the same question every few hours. A stated intent ("will do now") or a
reply is not completion. Only an explicit source status change completes or
cancels a task.

This is the single explicit exception to the "no periodic model review" rule,
requested by the user for this feature only. It does not enable the legacy
fixed-interval reminder loop, inbox polling or any other periodic model work.

## Mechanism

- **Cadence**: one explicitly configured Fleet role loop (operator config owned
  by the Fleet coordinator, not by this skill) starts a review pass about every
  2.5 hours. Fleet skips a pass while the assistant is busy and coalesces
  downtime, so there is no backlog replay and no successor reminder to lose.
- **Deterministic eligibility**: each pass runs
  `scripts/task_review.py --config <private-host-config>` with `{"action":"plan"}`.
  `plan` opens the configured schema2 source **read-only** (no source write,
  schema change or second writer) and updates only a private sidecar SQLite
  file bound to the source namespace. Each user has their own sidecar next to
  their own source.

  An open, unarchived, not-opted-out task is due for reconsideration when:
  - about 2 hours have **elapsed** since its last review, so a silent task comes
    back without any source change; or
  - it **meaningfully changed**: a new source revision or a change in
    prerequisite readiness, which brings the review forward to 30 minutes after
    the change is seen.

  At most 20 tasks are listed per pass, least recently reviewed first; the
  rest wait for the next pass. During confirmed quiet hours nothing is listed.
- **Accounting** is persisted per task: last review and outcome, last actual
  send (time, wire, outcome), unanswered-send count, total sends, the review
  rationale, opt-out, send claims and counted replies. Restart or replay never
  resets any of it.

## A review pass

1. `plan`. If `action` is `end_turn_quietly`, end the turn: no message, no
   inbox or history read.
2. For each listed task, compare its saved state with what was last said
   (`last_send_utc`, `unanswered_sends`, `last_outcome`) and, only when
   relevant, recent conversation. Use bounded ordinary source recovery.
   Decide whether a message would be genuinely useful *now*: new information,
   a newly ready step, an approaching deadline or a natural opportunity. Never
   re-ask an unanswered question in the same form. Coalesce tasks into one
   natural message.
3. Nothing useful: record the truthful outcome for all such tasks in ONE call,
   `{"action":"record","args":{"decisions":[{"task_id":…,"attempt_id":…,"outcome":"no_message"},…]}}`.
   You may add `next_review_utc` (30 min–60 days ahead) when a real later
   opportunity is known. `no_message` is accounting, not completion.
4. Useful, and `send_blocked` is null: call `begin_send` with the task's
   `claim_token` and `source_revision` from `plan`. It is a durable claim that,
   in one transaction and at the actual time, rechecks:
   - the source is still open at that revision and still eligible;
   - no exact-time timer or return owns the task;
   - confirmed quiet hours, the per-task cooldown and budget;
   - at most 3 claims per 2h across tasks, and no other claim in flight;
   - for `hh-` tasks, the role's own household `bind_reminder` + `due_guard`,
     re-reading source after the guard. The binding id is the namespaced
     `task-review:<attempt_id>`, so it never replaces an existing timer's
     household binding. Closed, stale and conflicted peer items are refused.

   Send only on `send: true`, through the normal configured route. Then call
   `send_result` with `sent` (+`wire_id`), `failed`, `not_sent` or
   `uncertain`.

## Send policy

- Per task, review messages back off from the actual send: 1d, then 2d, then
  4d. After 3 unanswered messages, no further message is allowed while the
  task is unchanged. The task is still reconsidered on every pass, but can
  only conclude `no_message`. A meaningful change after the last send
  re-allows a message. A source edit or peer projection never shortens the
  cooldown.
- `responded` (`task_id`, actual incoming `wire_id`, its `incoming_utc`) clears
  the unanswered count when the user actually engaged with the task after the
  last review message. Each wire counts once; older or replayed messages never
  reset a later budget.
- The send time recorded is the transport confirmation time, a conservative
  bound that is never earlier than the real send.
- `uncertain` keeps the claim in flight and the task held. Resolve it with
  `sent` (+`wire_id`) when history shows the message. Resolve it with
  `not_sent`/`failed` only when there is definite evidence: `evidence` set to
  `transport_rejected` or `never_submitted`. Absence from history alone keeps
  it held. Never resend blindly. Replaying `begin_send` with a used `attempt_id` never authorizes a
  second send.
- Opt-out: when the user asks not to be asked about a task, `record` `opt_out`;
  `opt_in` reverses it. Completion, cancellation or archive in source stops
  reviews automatically; an explicit reopen resumes them.
- Quiet hours must be user-confirmed and are interpreted in the user's
  confirmed timezone, never the host's. Record them with `set_quiet_hours`
  (`start`, `end` HH:MM, plus the actual confirmation `wire_id`), or an
  operator records `task_review_quiet_hours` with `basis: "user_confirmed"`
  in the private host config. Until then reviews may run and conclude
  `no_message`, but every send is held.

## Coexistence with timers and returns

Existing mechanisms keep their purpose and hold the task while they are live,
so review never creates a second channel for the same purpose:

- legacy source-bound reminders: an active mapping checked against the
  read-only delivery ledger, across the reminder and its follow-ups. It holds
  while that reminder is active and unexpired, or while any event is still
  admitted or in delivery (`pending`/`submitting`/`queued`/`uncertain`, or
  `delivery_status` `sending`/`uncertain`). A naturally finished reminder no
  longer holds the task;
- active message-return rules and contextual return decisions until their
  deadline;
- a prepared task-trigger reservation;
- a source schedule operation still `prepared` (not yet mapped);
- a native self-reminder registered with `cover`, which takes the actual
  receipt: `reminder_id`, `revision`, `due_at`, `expires_at`. The hold lasts
  until its late-window expiry. Cover is compare-and-swap:
  - re-register a newer revision of the same reminder to update it;
  - replacing it with a different reminder needs `replaces: "<id>@<revision>"`;
  - `clear` needs the covering id and revision;
  - stale or other receipts are refused.

At activation, register each existing native reminder that is bound to a task.

Legacy lookups fail closed: an unrecognized ledger schema or a query error
stops the pass instead of reading as "no timer".

When the mechanism ends, the task returns to normal review. Household items
remain governed by their peer protocol: stale, conflicted or closed items fail
the guard. Without a configured `household_helper`/`household_config`, `hh-`
sends are refused.

## Frequency and cost

- `plan` is a local SQLite scan, typically milliseconds.
- The loop starts about 9–10 short model turns per assistant per day, including
  empty and quiet-hour passes that end immediately. A pass with listed tasks
  reviews at most 20 compact entries and usually ends with one `record` call.
- Outward messages are a separate, much rarer decision: at most 3 per 2h
  across tasks, each task backed off as above. The steady state for an
  unanswered task is no further messages until something changes.
- `status` gives counts and the last review time without private titles.

## Activation

Activation is an operator procedure for an existing installation, separate from
installing these instructions:

1. Back up the private host config.
2. Add `task_review_db` (absolute path in the same private 0700 state
   directory). For a household-paired role, also add `household_helper` and
   `household_config`.
3. Run `task_review.py --config <cfg> --init` once.
4. The coordinator adds ONE manifest role loop targeting the assistant, with
   `interval: 150m`, `jitter` ≤ 10m, `enabled: true`, and a prompt limited to:

   > Periodic open-task review pass. Follow personal-assistant
   > references/task-review.md: run task_review plan; if it says
   > end_turn_quietly, end the turn without any message or reads; otherwise
   > review only the listed tasks and record outcomes. Never re-send uncertain
   > messages.

   Leave any legacy reminder loop disabled.

Rollback: disable the loop and remove the config key. The sidecar is then
inert; the source, preferences, timers and ledgers were never touched.
