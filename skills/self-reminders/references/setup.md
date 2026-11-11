# Native Fleet availability and legacy migration

For new use, install the skill in the harness's normal user-wide skill directory
and run the agent under Fleet with built-in `fleet_reminder_*` tools. No private
worker configuration, Python scheduler, systemd unit or recipient is needed.
Use the fixed-identity tools in SKILL.md. An operator can check a named managed
agent with `ours-fleet ours tools Name`; never use a different agent's tools as
though they were your own. Missing tools require a compatible managed Fleet
runtime; skill installation alone cannot upgrade a running supervisor.

## Existing standalone workers

Updating this skill does not stop services, move reminders or rewrite ledgers.
The old `scripts/self_reminders.py`, `scripts/fleet_wake.mjs` and `scripts/setup.py`
are retained only so installed legacy workers and deliberate migrations keep
working. Do not run setup, tick or serve for new scheduling.

A cutover must be an explicit per-agent operation, following the installed
Fleet package's `docs/self-reminders.md`, section "Migrating from the standalone
self-reminders worker". Locate that document in the installed Fleet package;
`ours-fleet docs` provides the supported reference. Keep private text inside its
own agent, not in shared reports. Essential order:

1. Confirm native tools actually work for that same agent. Record the old unit's
   file path and enabled/active states, then disable AND stop that exact producer
   persistently. Stopping alone allows it to return at boot.
2. Classify old current revisions and event eligibility. Terminal/expired records
   are not new schedules. A live submitting/queued/uncertain event needs
   reconciliation against actual agent history; do not copy it as a fresh wake.
3. Recreate only reconciled useful schedules through `fleet_reminder_create`:
   key `legacy:<old id>:r<old revision>`, old `context` -> `note`, old `timezone`
   -> `time_zone`. Past due dates require a new deliberate future scheduling
   decision, not replay. Retain source associations separately.
4. Verify native receipts, then cancel the corresponding old entries with exact
   revisions. Confirm no old active reminders or live unresolved events remain
   before removing the old service/config. Retain or delete old ledgers according
   to the owner's retention choice. Never point Fleet at a legacy SQLite ledger.

Rollback also requires reconciliation: never simply restart the old producer
when native reminders may already have fired. Follow the full Fleet rollback
matrix. No two producers should own the same reminder purpose.

Personal-assistant source, household, legacy outbound delivery and messaging
stores are separate. This skill update does not migrate or alter them. Existing
old `[self-reminders due event]` wakes still use their pinned legacy config and
inspect/complete workflow until explicitly reconciled; `[fleet-reminder]` wakes
use native inspect/ack. Do not mix event IDs, revisions or acknowledgements.
