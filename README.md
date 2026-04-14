# Migrate: Drupal to ContentStack Migration

> How to use this README: Follow **Quick Start** first, then use **Day-to-Day Commands** and **First-Week Path For A New Intern** as your checklist.

## Project Summary

This repository contains a Python migration tool that reads content from a Drupal database (MariaDB/MySQL-compatible schema) and creates equivalent structures in ContentStack using the Content Management API.

The script can run in two modes:
- Full migration: database content + file assets
- DB-only migration: database content only (no file uploads)

Assumptions used in this README:
- Audience: new intern with basic engineering background
- OS: macOS/Linux shell environment (commands use `make` and `.venv/bin/...` paths)

## What This Repository Contains

- A single migration entrypoint: `migrate.py`
- Dependency manifest: `requirements.txt`
- Automation commands: `Makefile`
- Environment template: `.env.example`
- Placeholder docs/work folders (`frontend/`, `quickstart/`) that are currently git-ignored in this repository state

## Tech Stack

- Language: Python 3
- Database access: `pymysql`
- CMS API: `requests` to ContentStack Management API
- Config loading: `python-dotenv`
- Drupal config parsing: `phpserialize`
- Rich text conversion: `beautifulsoup4` (HTML -> ContentStack JSON RTE)

## Prerequisites

- Python 3 installed (`python3` available on PATH)
- Access to Drupal DB host/credentials
- ContentStack API key + management token
- Optional for full migration: local filesystem access to Drupal public files directory

## Quick Start

```bash
cp .env.example .env
# edit .env with real credentials and paths

make install
make migrate
```

Expected outcome:
- Virtual environment `.venv` is created
- Dependencies are installed
- Migration logs print phase-by-phase progress
- `migration_records.json` is generated in repo root (used for rollback tracking)

DB-only mode:

```bash
make migrate-db-only
```

Direct script usage (equivalent):

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python migrate.py
.venv/bin/python migrate.py --db-only
```

## Environment Variables

Copy `.env.example` to `.env` and set these:

| Variable | Required | Description |
|---|---|---|
| `DB_HOST` | Yes | Drupal DB host |
| `DB_PORT` | No (default `3306`) | Drupal DB port |
| `DB_NAME` | Yes | Drupal DB name |
| `DB_USER` | Yes | Drupal DB user |
| `DB_PASSWORD` | Yes | Drupal DB password |
| `CONTENTSTACK_API_KEY` | Yes | ContentStack stack API key |
| `CONTENTSTACK_MANAGEMENT_TOKEN` | Yes | ContentStack management token |
| `CONTENTSTACK_BASE_URL` | No (default `https://api.contentstack.io/v3`) | Management API base URL |
| `DRUPAL_FILES_PATH` | Required for full asset migration | Absolute path to Drupal public files dir (for `public://...`) |
| `RATE_LIMIT_DELAY` | No (default `0.15`) | Delay between API requests in seconds |

## Day-to-Day Commands

| Command | Purpose | Expected Outcome |
|---|---|---|
| `make install` | Create `.venv` and install Python deps | `.venv` exists and deps installed |
| `make migrate` | Full migration (DB + assets) | Content types/entries/assets created in ContentStack |
| `make migrate-db-only` | Skip file upload phase | Taxonomy/paragraphs/nodes migrated without assets |
| `make clean` | Remove virtual environment | `.venv` removed |

`TODO(maintainers)`: Add standard test/lint/format/build commands if/when introduced.

## Repository Map

| Path | Purpose |
|---|---|
| `migrate.py` | Main migration script and orchestration logic |
| `requirements.txt` | Python dependencies |
| `Makefile` | Local automation for install/migrate/cleanup |
| `.env.example` | Environment variable template |
| `my-skill/repo-onboarding-publisher.md` | Currently empty placeholder markdown |
| `frontend/` | Present on disk but ignored by Git in current repo state |
| `quickstart/` | Present on disk but ignored by Git in current repo state |

## System Overview

High-level migration flow in `migrate.py`:

1. Load environment configuration.
2. Connect to Drupal DB and ContentStack API.
3. Optionally migrate assets from Drupal `file_managed` table by resolving `public://` file URIs.
4. Discover taxonomy vocabularies and migrate them to ContentStack content types + entries.
5. Discover paragraph types and convert them into ContentStack modular block schemas.
6. Discover Drupal node bundles, create ContentStack content types, and migrate node entries.
7. Add deferred reference fields after all content types exist.
8. Save created object IDs to `migration_records.json`.

Important boundaries:
- External DB: Drupal database schema and data quality
- External API: ContentStack rate limits and schema rules
- File system dependency: local access to Drupal public files for asset uploads

## Glossary

- Drupal bundle: A Drupal content subtype (for example, a node type).
- Paragraph type: Reusable nested content block type in Drupal.
- Taxonomy vocabulary: A Drupal term set used for classification.
- Content type (ContentStack): A schema defining entry fields.
- Entry (ContentStack): A content record within a content type.
- Asset (ContentStack): Uploaded file object (image/document/etc.).
- JSON RTE: JSON rich text format used by ContentStack editors.

## First-Week Path For A New Intern

1. Read `migrate.py` top-to-bottom once to understand phases and mappings.
2. Create `.env` from `.env.example` using sandbox/dev credentials.
3. Run `make install` and then `make migrate-db-only` first (safer than asset migration).
4. Review logs for any failed entries/content types and inspect field mapping behavior.
5. Validate generated content in a non-production ContentStack stack.
6. Run full `make migrate` only after DB-only behavior looks correct.
7. Propose small, testable improvements (for example: better logging granularity, explicit dry-run mode, or validation checks before API calls).

## Troubleshooting

- Error: missing environment variables
  - The script validates required keys at startup and exits if any are empty.
  - Fix: ensure `.env` exists and includes non-empty required values.

- DB connection/authentication failure
  - Fix: verify `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, and network access to the DB.

- Assets not uploading
  - Likely causes: `DRUPAL_FILES_PATH` incorrect or files missing for `public://...` URIs.
  - Fix: set `DRUPAL_FILES_PATH` to Drupal public files absolute path and verify file readability.

- API rate limit / transient failures
  - Script retries `429` with exponential backoff in API calls.
  - Fix: increase `RATE_LIMIT_DELAY` if needed for your stack limits.

## Where To Ask Questions / Ownership

`TODO(maintainers)`: Add team channel, primary code owner(s), and escalation path for migration incidents.
