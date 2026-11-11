# Ours Skills

Reusable agent skills for personal assistants built with ours.network.
The same skill folders can be installed for Claude Code and Codex.

## Skills

| Skill | Purpose |
| --- | --- |
| [personal-assistant](skills/personal-assistant/SKILL.md) | Private notes, tasks, contextual follow-ups and interactive Telegram widgets. |
| [self-reminders](skills/self-reminders/SKILL.md) | One-time internal wakes through Ours Fleet's built-in reminder tools. Required companion for personal-assistant. |

## Install by cloning

Clone this repository on the host where the agent runs:

```bash
git clone https://github.com/adapt-toolkit/ours-skills.git "$HOME/ours-skills"
```

For **Claude Code**, link both folders into the user-wide skills directory:

```bash
mkdir -p "$HOME/.claude/skills"
ln -s "$HOME/ours-skills/skills/personal-assistant" "$HOME/.claude/skills/personal-assistant"
ln -s "$HOME/ours-skills/skills/self-reminders" "$HOME/.claude/skills/self-reminders"
```

For **Codex**, use its user-wide skills directory:

```bash
mkdir -p "$HOME/.agents/skills"
ln -s "$HOME/ours-skills/skills/personal-assistant" "$HOME/.agents/skills/personal-assistant"
ln -s "$HOME/ours-skills/skills/self-reminders" "$HOME/.agents/skills/self-reminders"
```

These commands intentionally do not replace an existing skill. If one is already
installed, preserve it and review its local changes before switching to the clone.
Older Codex installations may use `~/.codex/skills`; avoid keeping duplicate
copies of the same skill in multiple discovery directories.
Start a new agent session after installation so it discovers both skills.

## Update manually

```bash
git -C "$HOME/ours-skills" pull --ff-only
```

Linked skills use the updated files. Start a new session to load the new
instructions. Updates to these files do not migrate private data, restart agents
or services, or enable integrations. Review storage or script changes before
applying them to an existing assistant installation.

There is no npm package, release/version automation or Ours installer integration.
The repository is the source; updates are managed manually for now.

## Configure an assistant

Installing a skill provides instructions and helper scripts, not a configured
assistant. Read [reproduction and setup](skills/personal-assistant/references/reproduction.md).

- Native reminders require a Fleet-managed agent with `fleet_reminder_*` tools.
- Telegram widgets require an approved Ours contact route advertising the widget
  commands through a compatible Telegram connector.
- Private source storage uses Python 3.9+, SQLite with FTS5 and IANA timezone data.
  Optional semantic features require separately configured local services/models;
  they are not installed or enabled by cloning this repository.
- Legacy worker scripts and migration guides are retained for existing
  installations. New reminders use Fleet's native tools, not a separate worker.

Keep credentials, contact bindings, identities, databases, notes, preferences and
host activation files **outside this repository**. The assets contain templates
with placeholders, not live configuration. Do not copy another user's state.

## Project policies

Community policies and the license are copied from
[ours-shared](https://github.com/adapt-toolkit/ours-shared):
[contributing](CONTRIBUTING.md), [security](SECURITY.md),
[trademarks](TRADEMARKS.md), and [commercial licensing](COMMERCIAL-LICENCE.md).

**Alpha software.** These skills are experimental and are not production-ready.
They have not been independently security-audited and are provided as is,
without warranty. Review scripts before granting access to private state.

Copyright 2026 Adapt Framework Solutions Ltd.
Licensed under [FSL-1.1-Apache-2.0](LICENSE): source-available during the FSL
period, with conversion to Apache 2.0 two years after publication of each version.
