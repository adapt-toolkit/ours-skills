# Reviewed local indexed-source cutover procedure

This is an operator procedure for an explicitly authorized migration of an existing installation. Review the exact skill bundle and operator code before use. Authorization belongs to the current user and operation; this document grants none. Do not run a migration merely to install or update skill instructions.

## Review packet and private parameters

The final joint manifest supplies the exact bundle, source helper, runtime and operator hashes; its manifest digest must also be independently approved. Candidate tests/evidence remain in the task workspace; live state and private checkpoint material stay in the configured private host state directory. These commands use shell variables containing operator-verified absolute paths, never JSON contents, route values, tokens or source records:

* `migration_bundle`: exact installed merged skill directory; checked against `migration_manifest` and `migration_manifest_sha` after managed install, before importing helpers.
* `migration_config`: current canonical private host config, owned mode600, preserves existing identity/route/namespace and all unrelated values.
* `migration_checkpoint`: NEW owned mode700 private directory in the same configured host state area. Do not reuse an earlier activation backup.
* `migration_preferences`, `migration_writer`: PA privately identifies the original curated preferences file and current JSON writer; both owned mode600 regular files. Paths may be supplied privately; do not print their contents.
* `migration_source_db`: NEW distinct absolute SQLite path under owned mode700 private parent. Existing unknown stores/schemas refuse; a recorded import_prepared empty target can resume only after exact path/hash/namespace and zero-schema checks.
* `migration_source_sha`, `migration_runtime_sha`: exact reviewed `scripts/source_store.py` and `scripts/reminder_tools.py` hashes from the joint manifest.
* `migration_preference_sha`, `migration_confirmed_kind`: exact reviewed preference helper hash and PA-verified technical enum denoting explicit user timezone confirmation. Never infer this enum from a timestamp or guess. Private timezone/wire/date values are not console arguments.

## Fresh pause and coherent preservation

Coordinator obtains a fresh explicit PA sole-writer pause ACK: source/preferences writes, reminder CRUD/follow-ups and ordinary intake stop. PA returns metadata only: fresh partition/field shape, pending contextual due/admitted drafts, submission/delivery uncertainty, and sole-writer state. Existing work continues until this ACK. Coordinator then stops the one helper through the supported management route and confirms it stopped. The legacy periodic model loop remains live/persistently disabled throughout. Pause includes all three stores and the external journal, not only the scheduler.

No uncertain attempt or admitted unsent contextual draft may be silently reset. Reconcile privately first, or HOLD the activation. The operator checks exclusive scheduler flock and ledger quietness independently. The quiescence flag below records already observed ACK/stop; it cannot produce or replace them.

Coordinator installs/applies only the exact reviewed merged skill using its supported management route while everything is paused, preserves the old writer file as an inert recovery artifact, then checks every manifest file/digest and the fixed service entry point. PA must adopt indexed intake/runtime as the sole source writer; the old positional JSON writer must not resume. No periodic loop, second helper or alternate sender is enabled. If installation fails, remain paused or recover the previous still-bound operation with current state under managed control; do not continue a partial cutover.

Run these sequentially, inspecting each bounded status before the next. Preserve full private snapshots; console output contains only counts/digests/booleans:

```bash
python3 "$migration_bundle/scripts/storage_migration.py" backup \
  --config "$migration_config" --checkpoint-dir "$migration_checkpoint" \
  --preferences "$migration_preferences" --writer "$migration_writer" \
  --bundle-root "$migration_bundle" --bundle-manifest "$migration_manifest" --bundle-sha "$migration_manifest_sha" \
  --operator-quiescence-ack

python3 "$migration_bundle/scripts/storage_migration.py" import \
  --config "$migration_config" --checkpoint-dir "$migration_checkpoint" --source-db "$migration_source_db" \
  --source-helper "$migration_bundle/scripts/source_store.py" --source-sha "$migration_source_sha" \
  --bundle-root "$migration_bundle" --bundle-manifest "$migration_manifest" --bundle-sha "$migration_manifest_sha" \
  --operator-quiescence-ack

python3 "$migration_bundle/scripts/storage_migration.py" prepare-ledger \
  --config "$migration_config" --checkpoint-dir "$migration_checkpoint" \
  --runtime "$migration_bundle/scripts/reminder_tools.py" --runtime-sha "$migration_runtime_sha" \
  --bundle-root "$migration_bundle" --bundle-manifest "$migration_manifest" --bundle-sha "$migration_manifest_sha" \
  --operator-quiescence-ack

python3 "$migration_bundle/scripts/storage_migration.py" verify \
  --config "$migration_config" --checkpoint-dir "$migration_checkpoint" \
  --source-helper "$migration_bundle/scripts/source_store.py" --source-sha "$migration_source_sha" \
  --bundle-root "$migration_bundle" --bundle-manifest "$migration_manifest" --bundle-sha "$migration_manifest_sha" \
  --operator-quiescence-ack

python3 "$migration_bundle/scripts/storage_migration.py" cutover \
  --config "$migration_config" --checkpoint-dir "$migration_checkpoint" \
  --source-helper "$migration_bundle/scripts/source_store.py" --source-sha "$migration_source_sha" \
  --bundle-root "$migration_bundle" --bundle-manifest "$migration_manifest" --bundle-sha "$migration_manifest_sha" \
  --operator-quiescence-ack
```

Between `import` and `prepare-ledger`, bind the existing confirmed timezone lookup if PA has verified its exact provenance contract. This changes private source binding metadata only, never preference values or source records; `verify` checks the saved binding. If provenance is missing/unqualified, leave `not_configured`, report that limitation and clarify timezone before any new timer; do not invent confirmation:

```bash
python3 "$migration_bundle/scripts/storage_migration.py" bind-preferences \
  --config "$migration_config" --checkpoint-dir "$migration_checkpoint" \
  --source-helper "$migration_bundle/scripts/source_store.py" --source-sha "$migration_source_sha" \
  --preference-sha "$migration_preference_sha" --confirmed-kind "$migration_confirmed_kind" \
  --bundle-root "$migration_bundle" --bundle-manifest "$migration_manifest" --bundle-sha "$migration_manifest_sha" \
  --operator-quiescence-ack
```

Backup compares a consistent current SQLite ledger with the saved ledger, recording every original table/column/count/digest, and stable source/preferences/writer/config bytes. Import transactionally preserves every original JSON field/opaque partition/order/ID/provenance/namespace; complete reverse export, field counts, digest, integrity and foreign keys match. It imports and verifies every existing ledger binding, without changing any ledger row, schedule or claim. Resolvable event/task references receive exact source mappings. Unresolved or nonstandard references are preserved only for explicit terminal reminder statuses in a separate private historical_terminal_bindings table, including the original ID/revision/reference/status, explicit reason and immutable canonical receipt/digest. These historical rows cannot become actionable source mappings, invent source records/revisions, or replay terminal claims. Any unresolved nonterminal binding HOLDs the whole binding transaction; ledger submission/delivery uncertainty and unsent contextual drafts still HOLD independently. Verification and cutover compare every mapping/history receipt again, reject overlaps/extras, and retain full historical rows in complete-state exports/SQLite backups. Undefined nonempty legacy tasks HOLD. Ledger preparation performs additive schema/index/derived queue setup only; every original ledger column remains equal. No recovery/admission/send runs during preparation. Config replacement is atomic and durable and changes only source_json/source_db/source_schema/source_journal.

Creation/import and config replacement have recorded crash stages. Reopening the same reviewed importer/path resumes only a recognizable planned empty/schema/imported store and verifies all bindings again; unknown/partial schemas HOLD. After config replacement, reconciliation accepts only the exact planned new binding and unchanged unrelated settings. Do not delete an interrupted target or restore stale state. If the initial backup interrupted before its checkpoint completed, retain the partial artifacts and repeat fresh preservation in a new private checkpoint directory under the continuing fresh pause.

### Corrected importer and retained partial evidence

The original v3 importer can commit a complete original source import but refuse historical terminal references before any binding commits. Its private checkpoint remains import_prepared. Keep this target/checkpoint and the complete original backup. A temporary code-only v2 recovery may resume current JSON intake after reciprocal current-state readbacks, PA adoption ACK and Coordinator lifecycle control; never restore stale source/config/ledger data. After any resumed writes, take a NEW coherent backup under a NEW fresh pause. Use a NEW source database path and retain the earlier partial target/checkpoint unbound; do not run later stages against the obsolete baseline.

Only while every original file/config/ledger field still equals the checkpoint under an actual continuing pause, a revised reviewed importer may explicitly continue that same partial target. Add `--resume-previous-source-sha 019b2eab5c3467bff5410796e24b8a6a9761f0328e07dde0ee76d083253605b5` to the reviewed import command. This import-only option recognizes exactly the prior frozen helper, import_prepared checkpoint/target, exact old or corrected schema, namespace, committed original import at generation1, complete original export/field counts/digest, integrity/foreign keys, and zero committed mappings/operations/history. It saves immutable private checkpoint-before-reviewed-import-repair.json and a consistent source-before-reviewed-import-repair.sqlite backup, comparing every original SQLite table/column/count/digest, before invoking the source helper's explicit atomic operator-only additive history-table extension. It then durably records the reviewed helper transition and backup provenance. Interrupted extension/hash transitions resume only after the same checks; backup/provenance changes HOLD. The option cannot authorize an unknown schema/helper, modify original records/history/FTS, upgrade ordinary intake implicitly, bypass quietness, or accept changed current source/ledger data.

## Readback and resume

Before resume, both Developers independently verify: complete installed manifest; exactly one source_db/schema2/namespace binding; preserved route/identity/preferences; complete imported original field counts/digest and mapping rows; all original paused ledger fields/counts/digests; unchanged original JSON and inert writer; the correct sole indexed writer; the single helper service command; the periodic loop still disabled. Shared evidence contains only safe booleans/counts/digests. PA ACKs the indexed sole-writer procedure before normal intake resumes. Coordinator starts/resumes only the existing helper and applies the reviewed PA skill/runtime through management. No timer is created for acceptance. Later helper readback checks one process, recovery guards and no idle model admission; legitimate current writes/terminal outcomes after resume are expected and must be distinguished from the paused baseline.

## Fixed-only fallback after new writes

Obtain another fresh PA pause ACK, stop the helper through Coordinator, reconcile uncertainty/admitted drafts, and verify the exact whole bundle again. Use the SAME checkpoint and current binding. Never switch to saved original JSON or restore saved ledger/writer:

```bash
python3 "$migration_bundle/scripts/storage_migration.py" export-current \
  --config "$migration_config" --checkpoint-dir "$migration_checkpoint" \
  --source-helper "$migration_bundle/scripts/source_store.py" --source-sha "$migration_source_sha" \
  --bundle-root "$migration_bundle" --bundle-manifest "$migration_manifest" --bundle-sha "$migration_manifest_sha" \
  --operator-quiescence-ack

python3 "$migration_bundle/scripts/storage_migration.py" fallback \
  --config "$migration_config" --checkpoint-dir "$migration_checkpoint" \
  --source-helper "$migration_bundle/scripts/source_store.py" --source-sha "$migration_source_sha" \
  --bundle-root "$migration_bundle" --bundle-manifest "$migration_manifest" --bundle-sha "$migration_manifest_sha" \
  --operator-quiescence-ack
```

This saves current full originals and complete internal source state, and consistent current source/ledger/external-journal SQLite backups. It compares every private table field/count/digest, then atomically sets contextual_enabled:false. The current indexed source/sole writer, all new writes, source history/mappings/receipts and current ledger dedup/outcomes remain authoritative. The runtime persists fixed-only policy before scheduling: contextual pending retries and new queues are held, contextual delivery/dismissal/follow-up refuse before claim, fixed reminders continue. Read back that policy and state before Coordinator resumes the one helper. Do not automatically re-enable contextual work or replay a terminal claim; re-enabling requires reviewed current state and managed policy transition. No destructive pruning is included.

Operator export/complete-state reconstruction is explicit private recovery work and may materialize a grown store/history. Generated artifact equality uses streaming canonical serialization and stable full-file hashes; it is not limited to the legacy2MiB input ceiling. The normal legacy/model input/output caps remain unchanged. A regression verifies current original and complete-history artifacts over2MiB, repeatable fallback, and complete current semantic-state equality.

Every export/fallback invocation allocates a new immutable private recovery generation, retaining earlier and interrupted evidence. Source generation alone cannot identify a newer fixed timer or delivery outcome. A regression verifies that a later ledger-only fixed send with unchanged source obtains a fresh backup and retains its current terminal wire/dedup fields.
