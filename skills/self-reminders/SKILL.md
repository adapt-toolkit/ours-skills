---
name: self-reminders
description: Schedule one-time reminders to yourself through built-in Ours Fleet tools, receive a due-time agent turn, and decide what to do. Use for returning to work at a date and time without a separate worker, messaging destination, or model polling.
---

Use the agent's own `fleet_reminder_*` tools. Fleet's existing supervisor owns
scheduling for persistent, temporary and task-owned agents, in Claude or Codex.
There is no worker, Python configuration, systemd unit or messaging destination
to set up. If the tools are missing or return `reminders_unavailable` or
`runtime_not_ready`, report the Fleet availability issue; do not create a custom
worker or substitute a periodic model loop.

A reminder wakes YOU. It sends nothing to another person. A saved note is context,
not new authorization to send, merge, deploy or perform unrelated work.

## Schedule and manage

- `fleet_reminder_create`: `request_key`, `title`, `local_time`, `time_zone`;
  optional `note`, `fold`, `late_window_seconds`. Retain the returned reminder's
  `id`, `revision`, `due_at` and `expires_at`.
- `fleet_reminder_list`: `state` is `open` (default), `closed` or `all`;
  optional `limit` (1–100) and `after` cursor. Follow `more`/`after` only when
  needed. A page missing an item is not a reason to recreate it.
- `fleet_reminder_update`: `id`, `expected_revision` and only the changed fields.
  Use it to postpone a reminder; the old revision's wake becomes void.
- `fleet_reminder_cancel`: `id`, `expected_revision`.

Resolve relative dates using the actual user/task timezone. `local_time` is a
full calendar date and wall-clock time WITHOUT offset; `time_zone` is an IANA
zone. Never infer the user's timezone from the host. DST gaps are rejected;
ambiguous times require `fold: 0` (first) or `1` (second). Times must be future
and within five years. The late window is 60–604800 seconds, default 3600.

Use a stable `request_key` for one scheduling decision. Retry an uncertain create
with the SAME key and input; different input with that key conflicts. Update or
cancel with the latest revision; on `stale_revision`, inspect current state
rather than overwriting it. Fleet retains at most 100 open and 200 recent closed
reminders, so old keys are not an unlimited deduplication history.

Example tool input (illustrative only; choose the real future date from context):

```json
{
  "request_key": "release-check-actual-request-id",
  "title": "Check release CI",
  "note": "Inspect the existing PR. Report through the task room if useful; do not merge.",
  "local_time": "2027-01-20T16:00:00",
  "time_zone": "Europe/London"
}
```

Call `fleet_reminder_create` with that object. Do not schedule examples as real
reminders. Prefer a useful finite return over polling or sleeping in a turn.

## Handle a wake

On `[fleet-reminder]`, call `fleet_reminder_inspect` with its `event_id`. Check
`current`, `eligible`, revision and current task/source context. Ignore stale,
cancelled or expired purposes. Decide what to do, including taking no external
action, then acknowledge a handled eligible event with:

```json
{"event_id":"<event_id from wake>","expected_revision":1}
```

Use `fleet_reminder_ack`, with the ACTUAL revision from the wake/inspection.
Repeated acknowledgement is safe. It records internal handling, not task
completion, message delivery or human receipt. To postpone instead, update the
reminder; do not acknowledge the now-invalidated old wake.

Perform any useful authorized external action through normal tools, tracking its
outcome separately. Do not repeat an uncertain send. If `list` reports
`uncertain_events`, reconcile your own event/conversation records before
acknowledging or making a new scheduling decision; never replay blindly.

## Lifecycle and delivery

Fleet generates no model turns while waiting. A due wake gets its own turn at an
idle boundary after startup succeeds; it does not interrupt current work. Busy or
offline agents may be delayed. Stopping/pausing/restarting preserves reminders;
reminders never start a stopped agent. After recovery, reminders outside their
late window expire. Removing/retiring an agent or finishing/deleting its task
cancels its reminders and removes their state. A replacement identity does not
inherit them.

Scheduling, releasing a wake to the model, acknowledgement and any outward
message are separate outcomes. Report only the outcomes actually confirmed.
Read [setup and legacy migration](references/setup.md) only for missing tools or
an installation that already has standalone workers. Old scripts retained in
this directory are compatibility artifacts, not the scheduling interface.
