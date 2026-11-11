# Self-reminders companion dependency

New installations must co-install the `personal-assistant` and `self-reminders`
skill directories and use a Fleet-managed agent exposing `fleet_reminder_*`.
Fleet owns scheduling; do not provision a Python worker, user service or private
self-reminder configuration. No message destination is needed for scheduling or
internal wake acceptance. Configure Ours/Telegram separately only for messages
the assistant chooses to send. See
[self-reminders integration](self-reminders-integration.md) for the native default.
The legacy runtime/source-ledger setup below applies only when retaining existing
legacy reminders or explicitly using the older outbound-delivery wrapper; it is
not a prerequisite for new self-reminders. Do not copy another user's stores.

# Fresh consenting user

Copy the reviewed package to the user's local skill directory. Default requested location is `.codex/skills/personal-assistant`; current OpenAI docs also describe `.agents/skills`, so verify discovery against the installed version. Avoid duplicate skill names and forced fresh sessions; managed lifecycle stays with the supervisor.

Run `scripts/new_workspace.py --workspace <fresh-directory>`. It refuses existing directories and creates empty source DB, curated preferences, event/reminder mapping and disabled activation template. Each source DB gets an independent namespace. Never clone live state, ledgers, contacts or connector credentials.

Privately configure generated `activation.json` with the independent assistant identity, list ID, confirmed IANA timezone, approved Ours contact and reviewed reminder runtime paths. Keep unknown fields unset and Telegram unverified. `source.sqlite3` holds tasks/events; `preferences.json` holds curated preferences; runtime owns separate scheduling/admission/delivery DB.

Copy `assets/reminder-host.template.json` into the private user workspace and replace its paths/identity/route with verified host values; keep it mode600. For a fresh indexed SQLite workspace set `source_db`, `source_schema:2`, the new canonical `source_namespace`, and a private `source_journal` path for this user. Use the schema2 [bounded intake API](storage.md), never copy another user's database. For an existing legacy JSON workspace, preserve its source and writer instead: remove `source_db`, privately set `source_json` and generate a canonical UUID `source_namespace` once, preserving it on upgrades. The JSON adapter currently qualifies verified events only and holds undefined task records; preferences stay separate. Do not initialize the SQLite source tutorial over existing JSON state. The active assistant CID is pinned in `expected_agent_cid` and verified before due admission and send. Use `assets/reminders.service.template` for exactly one unique per-user deterministic service through supported host management. Replace its entrypoint/config paths; enable only after reciprocal review. Preserve existing state, keep periodic model loops disabled and unrelated loops unchanged. Reconcile prior pending/delivered reminders instead of bulk-importing examples. Supervisor applies skill through a contextual supported wake without resetting live state. Read [reminder operations](reminders.md) for exact CLI and reconciliation limits. To roll back, stop/disable only this new worker and retain both stores/ledgers; never reenable the rejected periodic model loop.

Telegram needs the consenting user's own bot and verified recipient binding through existing connector; see [setup](telegram.md). No custom transport. Qualify fixtures, then one short labeled reminder through approved route. Record hashes, loaded versions, zero idle/one due admission, send result and remaining limits; use [trial](trial.md) before reproducing for others.

For a reviewed contextual upgrade, migrate only after both Developers inspect the candidate and the supervisor accepts exact artifact hashes. Legacy reminder rows retain fixed mode and zero follow-up budget. Preserve private permanent source/preferences/schedule/admission/delivery stores. Never open/migrate a live DB just to test the candidate, and never replay a confirmed old event. Stop only the single helper for an approved atomic code/config change and restart it through the supervisor; persistent assistant state is retained. Contextual fixture PASS is separate from one coordinated real due/receipt qualification.
