# Personal assistant + self-reminders

`self-reminders` is a required companion skill for new timed reminders. Keep the
personal source store/private memory separate from Fleet-owned reminder state.
Use native `fleet_reminder_*` tools; no worker, config or service setup is needed.
The scheduler wakes this assistant; it has no recipient and cannot send messages.

## New reminder

1. Capture any real task/event/note through the configured private source API,
   preserving existing source IDs, revisions and ordinary intake accounting.
   A source record alone is not a scheduled wake.
2. Use `fleet_reminder_create` with a stable request key, actual title, explicit
   date/time (`local_time`) and confirmed IANA `time_zone`. For preparation, compute time from the
   real event and agreed effort/buffer, including DST as usual. Put the minimum
   useful source reference and review purpose in its private `note`; saved
   context is data, never new execution authority.
3. Retain the returned self-reminder ID/revision and its source association through
   the agent's normal private notes/task mechanism. Do not insert into legacy
   source schedule journals or reminder-mapping tables by hand: they expect a
   different runtime receipt/schema. Report partial source/timer failures rather
   than claiming atomic commitment across stores.
4. Correct/cancel the self-reminder with its exact ID/revision if the source
   changes. A queued old revision fails due-time eligibility. Recheck source even
   if a cancellation write failed.

New self-wakes use the companion directly; legacy `finish_return`,
`create_followup`, `bind_reminder` and source-journal materialization do not create
self-reminders receipts. Use those paths only for actual old events. Existing
finite-purpose/spacing/user preference rules still govern proactive usefulness;
the generic scheduler does not reset a purpose episode or invent those policies.

## Due wake

For `[fleet-reminder]`, inspect the exact event with `fleet_reminder_inspect` and revalidate revision, eligibility and cancellation. Recover current relevant
private source through its ordinary bounded read interface, not a fabricated
legacy event-bound reminder ticket. Suppress completed/cancelled/stale purposes.
Acknowledge using `fleet_reminder_ack` with the exact event ID/revision. This handles only the internal wake;
it does not claim a task completed or a user was notified.

Independently decide whether a message is useful and authorized. Use separately
configured Ours/Telegram tools and destination rules if sending. No outbound
route may be required to schedule/handle a self-wake. Track actual send outcomes
separately; do not replay uncertain sends.

## Existing installations

Preserve legacy source/admission/send ledgers and outstanding events. Do not bulk
copy them to the new database or enable a second timer for the same purpose.
Use native Fleet tools for new schedules. Retain existing old producers only for
outstanding events until explicitly reconciled using the companion migration
reference. Old `[self-reminders due event]` wakes use old inspect/complete; new
`[fleet-reminder]` wakes use native inspect/ack. Never mix their receipts. Share neither personal state nor
recipient/credential bindings in skill archives. Distinguish skill installation,
native tool availability, schedule persistence, actual agent wake/acknowledgement and
optional external delivery in setup reports.
