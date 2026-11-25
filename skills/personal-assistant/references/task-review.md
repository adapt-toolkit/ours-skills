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

`scripts/task_review.py --config <private-host-config>` reads one JSON request
`{"action": ..., "args": {...}}` on stdin. It opens the configured schema2
source **read-only**: no source write, no schema change, no second writer.
Per-task review state lives in a separate private sidecar SQLite file bound to
the source namespace. Each user has their own sidecar next to their own source.
The script never sends anything.

- **Deterministic eligibility (`plan`)** is a cheap local scan. It runs only
  inside a turn the assistant is already in. An open, unarchived, not-opted-out
  task becomes eligible:
  - by **elapsed time**, when its persisted `next_eligible_utc` arrives; or
  - by a **meaningful change**: a new source revision or a change in
    prerequisite readiness. This brings the review forward to at most 3 hours
    after the change is observed.

  Silent tasks therefore come back without any source hash change. Tasks
  first seen are staggered 24–72 hours out, so activating the feature on a
  large backlog does not cause a burst.
- **Model wake** comes from ONE native `fleet_reminder_*` self-reminder (the
  "review chain"). `plan` returns the exact `schedule` to apply (`create`,
  `update` or `cancel`). Its time is the earliest predicted eligibility,
  spaced ≥2h after the previous review wake and moved out of quiet hours. Its
  late window is 7 days, so downtime yields at most ONE late catch-up wake;
  longer outages expire it. A wake is a *prediction*: tasks may have changed
  or completed, so always `plan` again first.
- **Accounting** is persisted per task: last review and outcome, last actual
  send (time, wire, outcome), unanswered-send count, total sends,
  `next_eligible_utc`, a reason (rationale) and opt-out. Restart or replay
  never resets any of it.

## On a review wake (`[fleet-reminder]` whose note starts `task_review`)

1. `fleet_reminder_inspect`. If the reminder is not current, ack it and stop.
2. `plan`. **Apply its `schedule` first** (create/update/cancel the one chain
   reminder). Then record the receipt with `chain` (`reminder_id`, `revision`,
   `request_key`, `due_at`), or with `{"cancelled": true}` after a cancel. This
   keeps the chain alive even if the rest of the turn fails. `plan` durably
   records the create intent before you call Fleet. Until the receipt is
   recorded, it repeats the identical request (same key and input), so a lost
   receipt never creates a duplicate reminder.
3. For each `eligible` task, recover bounded current context through the
   ordinary source interface. Compare it with what was last said
   (`last_send_utc`, `unanswered_sends`, `last_outcome`) and with recent
   conversation when that is relevant. Decide whether a message would be
   genuinely useful *now*: new information, a newly ready step, an approaching
   deadline, or a natural opportunity. Do not re-ask an unanswered question in
   the same form. Coalesce several tasks into one natural message.
4. Not useful: `record` `no_message` (reconsider in 24h by default; you may set
   `next_review_utc` 30 min–60 days ahead for a real contextual opportunity)
   or `deferred` (2h). This is truthful accounting, not completion.
5. Useful: `begin_send` with the task's `claim_token` and `source_revision`
   from `plan`. It rechecks everything at the actual time: source still open
   at that revision, eligibility, confirmed quiet hours, the send cooldown,
   at most 3 claims per 2h, no other claim in flight, and, for `hh-` tasks,
   the role's own household `bind_reminder`/`due_guard`. Send only if it
   returns `send: true`, through the normal configured route. Then call
   `send_result` with `sent` (+`wire_id`), `failed`, `not_sent` or
   `uncertain`.
6. `fleet_reminder_ack` the wake.

## Send policy

- After a sent review message the task's sends back off: 1d, then 2d, then 4d.
  After 3 unanswered sends, no further review message is allowed while the
  task is unchanged. It is still reconsidered every 14 days, because silence
  is not completion, but each review can only conclude `no_message`. A
  meaningful change re-allows ONE send: a new source revision or a change in
  prerequisite readiness after the last send. A source edit or peer projection
  brings the *review* forward but never shortens the send cooldown.
- `responded` (`task_id`, actual incoming `wire_id`) clears the unanswered
  count when the user actually engaged with the task. Each wire counts once,
  and replaying it never resets a later budget.
- `uncertain` keeps the claim in flight and the task held. Check your own
  outbound history, then `send_result` `sent` or `not_sent`. Never resend
  blindly. `begin_send` with an already used `attempt_id` never authorizes a
  second send.
- Opt-out: when the user asks to stop being asked about a task, `record`
  `opt_out`. `opt_in` reverses it. Completion or cancellation in source stops
  reviews automatically; reopening resumes them.
- Quiet hours: sends require user-confirmed quiet hours. Set them with
  `set_quiet_hours` (`start`, `end` HH:MM in the user's confirmed timezone,
  plus the actual confirmation `wire_id`), or an operator can record
  `task_review_quiet_hours` with `basis: "user_confirmed"` in the private host
  config. Until then wakes are placed outside 21:00–09:00 local and reviews
  can still conclude `no_message`, but every send is held. The timezone must
  be the user's confirmed preference, never the host zone.

## Coexistence with timers and returns

Exact-time timers and contextual returns keep their own purpose. A task with an
active legacy `reminder_mappings` row is held while that timer is live. For a
native self-reminder that already covers a task, call `cover` (`task_id`,
`reminder_id`, `due_utc`); the review waits until one hour after it fires.
`cover` with `clear: true` removes it. Never schedule a second channel for the
same purpose. Household items stay governed by their peer protocol: stale,
conflicted or closed items fail the guard and are not sent. Without a
configured `household_helper`/`household_config`, `hh-` sends are refused.

## Frequency and cost

- `plan` is a local SQLite scan, typically milliseconds. It costs no model
  turn by itself.
- Model wakes happen only at predicted eligibility. They are ≥2h apart,
  outside quiet hours, and pull in tasks due within the next 2h, so that is
  at most ~6–7 wakes on a full day. With few open tasks and default 24h
  reconsideration, expect about 1–3 per day. With no open tracked tasks there
  is no review wake at all.
- Outward messages are a separate, rarer decision: at most 3 per 2h, each
  task backed off as above.
- Operators can inspect counts with `status` (no private titles).

## Activation

Activation is an operator procedure for an existing installation, separate from
installing these instructions:

1. Back up the private host config.
2. Add `task_review_db` (absolute path in the same private 0700 state
   directory). For a household-paired role, also add `household_helper` and
   `household_config`.
3. Run `task_review.py --config <cfg> --init` once.
4. On the assistant's next turn, run `plan` and create the first chain
   reminder.
5. The coordinator adds the daily backstop loop described above.

Rollback: remove the config key, cancel the chain reminder and disable the backstop loop.
The sidecar is then inert; the source and ledgers were never touched.

## Chain recovery (autonomous backstop)

A one-shot chain alone can be lost in two cases: a wake expires after an outage
longer than its 7-day window, or an admitted turn dies before it creates the
successor. Installations therefore pair the chain with ONE daily backstop: a
Fleet manifest loop targeted at the assistant role, with `interval: 24h` and a
jitter of up to 1h. Its prompt is limited to repair:

> Task-review backstop. Run `task_review.py status`. If `repair_needed` is
> true, run `plan`, apply its `schedule` and record `chain`. Do nothing else:
> no review, no message, no inbox or history read.

This costs one short model turn per day per assistant, including empty
passes. Fleet loops skip busy turns and coalesce downtime, so it never replays.
The loop is operator configuration owned by the Fleet coordinator; this skill
does not enable it. Without the loop, the chain is still repaired on the next
ordinary user-message turn: run `status` when the chain was not seen current
this session.

