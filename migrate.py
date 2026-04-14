#!/usr/bin/env python3
"""
Dynamic Drupal-to-ContentStack Migration Script.

Discovers content types, fields, taxonomy, media, and paragraphs from a Drupal
MariaDB database and migrates them into ContentStack via the Management API.

Usage:
    pip install -r requirements.txt
    cp .env.example .env   # fill in real values
    python migrate.py              # full migration (DB + files)
    python migrate.py --db-only    # migrate only DB contents, skip file assets
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import uuid
from pathlib import Path

import pymysql
import pymysql.cursors
import phpserialize
import requests
from bs4 import BeautifulSoup, NavigableString
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "charset": "utf8mb4",
}

CS_API_KEY = os.getenv("CONTENTSTACK_API_KEY")
CS_MANAGEMENT_TOKEN = os.getenv("CONTENTSTACK_MANAGEMENT_TOKEN")
CS_BASE_URL = os.getenv("CONTENTSTACK_BASE_URL", "https://api.contentstack.io/v3").rstrip("/")
DRUPAL_FILES_PATH = os.getenv("DRUPAL_FILES_PATH", "")
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", 0.15))
RECORDS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migration_records.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uid() -> str:
    """Generate a short unique id suitable for JSON RTE nodes."""
    return uuid.uuid4().hex[:12]


def _sanitize_uid(name: str) -> str:
    """Convert a Drupal machine name to a valid ContentStack UID."""
    uid = re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_")
    # ContentStack UIDs must start with a letter
    if uid and uid[0].isdigit():
        uid = "d_" + uid
    return uid[:60] or "unnamed"


class DB:
    """Thin wrapper around pymysql for convenience."""

    def __init__(self):
        self.conn = pymysql.connect(
            **DB_CONFIG,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def query(self, sql: str, params=None) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def query_one(self, sql: str, params=None) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def tables_like(self, pattern: str) -> list[str]:
        rows = self.query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name LIKE %s ORDER BY table_name",
            (pattern,),
        )
        return [r["table_name"] for r in rows]

    def columns_of(self, table: str) -> list[dict]:
        return self.query(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = %s ORDER BY ordinal_position",
            (table,),
        )

    def close(self):
        self.conn.close()


class ContentStackAPI:
    """Wrapper for ContentStack Content Management API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "api_key": CS_API_KEY,
            "authorization": CS_MANAGEMENT_TOKEN,
            "Content-Type": "application/json",
        })

    # -- low-level ---------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> dict:
        url = f"{CS_BASE_URL}/{path.lstrip('/')}"
        for attempt in range(5):
            resp = self.session.request(method, url, **kwargs)
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning("Rate limited. Retrying in %ds...", wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                log.error("API %s %s -> %s: %s", method, path, resp.status_code, resp.text[:500])
                resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp.json()
        raise RuntimeError(f"Max retries exceeded for {method} {path}")

    def get(self, path: str, **kw) -> dict:
        return self._request("GET", path, **kw)

    def post(self, path: str, payload: dict, **kw) -> dict:
        return self._request("POST", path, json=payload, **kw)

    def put(self, path: str, payload: dict, **kw) -> dict:
        return self._request("PUT", path, json=payload, **kw)

    # -- content types -----------------------------------------------------

    def get_content_types(self) -> list[dict]:
        data = self.get("content_types")
        return data.get("content_types", [])

    def create_content_type(self, ct: dict) -> dict:
        log.info("Creating content type: %s", ct.get("uid"))
        return self.post("content_types", {"content_type": ct})

    def update_content_type(self, uid: str, ct: dict) -> dict:
        log.info("Updating content type: %s", uid)
        return self.put(f"content_types/{uid}", {"content_type": ct})

    # -- entries -----------------------------------------------------------

    def create_entry(self, content_type_uid: str, entry: dict, locale: str = "en-us") -> dict:
        log.info("Creating entry in %s: %s", content_type_uid, entry.get("title", "")[:60])
        return self.post(
            f"content_types/{content_type_uid}/entries?locale={locale}",
            {"entry": entry},
        )

    def update_entry(self, content_type_uid: str, entry_uid: str, entry: dict, locale: str = "en-us") -> dict:
        return self.put(
            f"content_types/{content_type_uid}/entries/{entry_uid}?locale={locale}",
            {"entry": entry},
        )

    # -- assets ------------------------------------------------------------

    def upload_asset(self, filepath: str, title: str = "", folder_uid: str = "") -> dict:
        log.info("Uploading asset: %s", Path(filepath).name)
        headers = {"api_key": CS_API_KEY, "authorization": CS_MANAGEMENT_TOKEN}
        data = {}
        if title:
            data["asset[title]"] = title
        if folder_uid:
            data["asset[parent_uid]"] = folder_uid
        with open(filepath, "rb") as f:
            resp = requests.post(
                f"{CS_BASE_URL}/assets",
                headers=headers,
                data=data,
                files={"asset[upload]": (Path(filepath).name, f)},
            )
        if resp.status_code == 429:
            time.sleep(2)
            return self.upload_asset(filepath, title, folder_uid)
        resp.raise_for_status()
        time.sleep(RATE_LIMIT_DELAY)
        return resp.json().get("asset", {})

    def delete_entry(self, content_type_uid: str, entry_uid: str) -> dict:
        url = f"{CS_BASE_URL}/content_types/{content_type_uid}/entries/{entry_uid}"
        for attempt in range(5):
            resp = self.session.delete(url)
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning("Rate limited. Retrying in %ds...", wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                log.error("DELETE entry %s/%s -> %s: %s", content_type_uid, entry_uid, resp.status_code, resp.text[:500])
                resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp.json()
        raise RuntimeError(f"Max retries exceeded for DELETE entry {entry_uid}")

    def delete_asset(self, asset_uid: str) -> dict:
        url = f"{CS_BASE_URL}/assets/{asset_uid}"
        for attempt in range(5):
            resp = self.session.delete(url)
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning("Rate limited. Retrying in %ds...", wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                log.error("DELETE asset %s -> %s: %s", asset_uid, resp.status_code, resp.text[:500])
                resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp.json()
        raise RuntimeError(f"Max retries exceeded for DELETE asset {asset_uid}")

    def delete_content_type(self, uid: str) -> dict:
        url = f"{CS_BASE_URL}/content_types/{uid}"
        for attempt in range(5):
            resp = self.session.delete(url)
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning("Rate limited. Retrying in %ds...", wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 400:
                log.error("DELETE content_type %s -> %s: %s", uid, resp.status_code, resp.text[:500])
                resp.raise_for_status()
            time.sleep(RATE_LIMIT_DELAY)
            return resp.json()
        raise RuntimeError(f"Max retries exceeded for DELETE content_type {uid}")

    def create_folder(self, name: str) -> dict:
        log.info("Creating asset folder: %s", name)
        return self.post("assets/folders", {"asset": {"name": name}})


# ---------------------------------------------------------------------------
# Drupal schema introspection
# ---------------------------------------------------------------------------

# Standard columns present in every Drupal dedicated field table
FIELD_TABLE_META_COLS = {"bundle", "deleted", "entity_id", "revision_id", "langcode", "delta"}


def discover_content_types(db: DB) -> list[str]:
    """Return all node bundle machine names."""
    rows = db.query("SELECT DISTINCT type FROM node ORDER BY type")
    return [r["type"] for r in rows]


def discover_field_tables(db: DB, entity_type: str = "node") -> dict[str, list[str]]:
    """Return {table_name: [value_columns]} for every dedicated field table."""
    tables = db.tables_like(f"{entity_type}__field_%")
    result = {}
    for tbl in tables:
        cols = db.columns_of(tbl)
        value_cols = [c["column_name"] for c in cols if c["column_name"] not in FIELD_TABLE_META_COLS]
        result[tbl] = value_cols
    return result


def discover_field_config(db: DB, entity_type: str = "node") -> dict[str, dict]:
    """Parse Drupal config table for field.storage entries to get field type metadata."""
    config_table_exists = db.query(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='config'"
    )
    if not config_table_exists:
        return {}
    rows = db.query(
        "SELECT name, data FROM config WHERE name LIKE %s",
        (f"field.storage.{entity_type}.%",),
    )
    result = {}
    for row in rows:
        try:
            raw = row["data"]
            if isinstance(raw, memoryview):
                raw = bytes(raw)
            elif isinstance(raw, str):
                raw = raw.encode("latin-1")
            parsed = phpserialize.loads(raw, decode_strings=True)
            field_name = parsed.get("field_name", row["name"].split(".")[-1])
            result[field_name] = {
                "type": parsed.get("type", "string"),
                "cardinality": parsed.get("cardinality", 1),
                "settings": parsed.get("settings", {}),
            }
        except Exception as exc:
            log.debug("Could not parse config %s: %s", row["name"], exc)
    return result


def discover_field_instances(db: DB, entity_type: str = "node") -> dict[str, dict[str, list[str]]]:
    """Return {bundle: {field_name: [target_bundles]}} from field.field config entries."""
    config_table_exists = db.query(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='config'"
    )
    if not config_table_exists:
        return {}
    rows = db.query(
        "SELECT name, data FROM config WHERE name LIKE %s",
        (f"field.field.{entity_type}.%",),
    )
    result: dict[str, dict[str, list[str]]] = {}
    for row in rows:
        try:
            raw = row["data"]
            if isinstance(raw, memoryview):
                raw = bytes(raw)
            elif isinstance(raw, str):
                raw = raw.encode("latin-1")
            parsed = phpserialize.loads(raw, decode_strings=True)
            bundle = parsed.get("bundle", "")
            field_name = parsed.get("field_name", "")
            settings = parsed.get("settings", {})
            handler_settings = settings.get("handler_settings", {})
            target_bundles = list((handler_settings.get("target_bundles") or {}).values())
            result.setdefault(bundle, {})[field_name] = target_bundles
        except Exception:
            pass
    return result


def discover_taxonomy_vocabularies(db: DB) -> list[str]:
    has_table = db.query(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='taxonomy_term_data'"
    )
    if not has_table:
        return []
    rows = db.query("SELECT DISTINCT vid FROM taxonomy_term_data ORDER BY vid")
    return [r["vid"] for r in rows]


def discover_paragraph_types(db: DB) -> list[str]:
    has_table = db.query(
        "SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='paragraphs_item'"
    )
    if not has_table:
        return []
    rows = db.query("SELECT DISTINCT type FROM paragraphs_item ORDER BY type")
    return [r["type"] for r in rows]


# ---------------------------------------------------------------------------
# Drupal → ContentStack field type mapping
# ---------------------------------------------------------------------------

DRUPAL_TO_CS_FIELD = {
    "string": ("text", {}),
    "string_long": ("text", {"field_metadata": {"multiline": True}}),
    "text": ("text", {"field_metadata": {"allow_rich_text": True}}),
    "text_long": ("json", {"field_metadata": {"allow_json_rte": True, "rich_text_type": "advanced"}}),
    "text_with_summary": ("json", {"field_metadata": {"allow_json_rte": True, "rich_text_type": "advanced"}}),
    "integer": ("number", {}),
    "float": ("number", {}),
    "decimal": ("number", {}),
    "boolean": ("boolean", {}),
    "datetime": ("isodate", {}),
    "timestamp": ("isodate", {}),
    "created": ("isodate", {}),
    "changed": ("isodate", {}),
    "uri": ("text", {}),
    "email": ("text", {}),
    "telephone": ("text", {}),
    "link": ("link", {}),
    "image": ("file", {}),
    "file": ("file", {}),
    "entity_reference": ("reference", {}),
    "entity_reference_revisions": ("reference", {}),
    "list_string": ("text", {"display_type": "dropdown"}),
    "list_integer": ("number", {"display_type": "dropdown"}),
    "list_float": ("number", {"display_type": "dropdown"}),
    "color_field": ("text", {}),
    "geofield": ("json", {}),
    "metatag": ("group", {}),
}


def map_drupal_field(field_name: str, drupal_type: str, value_cols: list[str], cardinality: int) -> dict:
    """Build a ContentStack schema field definition from Drupal field metadata."""
    cs_type, extra = DRUPAL_TO_CS_FIELD.get(drupal_type, ("text", {}))

    # If it's an entity reference with a target_id column and no richer info, use reference
    if drupal_type in ("entity_reference", "entity_reference_revisions"):
        cs_type = "reference"

    # For image fields detected by column heuristic
    if any(c.endswith("_alt") for c in value_cols) and any(c.endswith("_target_id") for c in value_cols):
        cs_type = "file"

    display_name = field_name.replace("field_", "").replace("_", " ").title()
    uid = _sanitize_uid(field_name)

    field_def = {
        "display_name": display_name,
        "uid": uid,
        "data_type": cs_type,
        "mandatory": False,
        "unique": False,
        "multiple": cardinality != 1,
    }
    field_def.update(extra)
    return field_def


# ---------------------------------------------------------------------------
# HTML → JSON RTE conversion
# ---------------------------------------------------------------------------

BLOCK_TAGS = {"p", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "ul", "ol", "li", "pre", "hr", "table", "thead", "tbody", "tr", "td", "th"}
INLINE_MARKS = {"strong": "bold", "b": "bold", "em": "italic", "i": "italic", "u": "underline", "s": "strikethrough", "del": "strikethrough", "code": "code"}


def html_to_json_rte(html: str) -> dict:
    """Convert an HTML string into ContentStack JSON RTE format."""
    if not html or not html.strip():
        return {"type": "doc", "uid": _uid(), "children": [{"type": "p", "uid": _uid(), "children": [{"text": ""}]}]}

    soup = BeautifulSoup(html, "html.parser")
    children = _convert_children(soup)
    if not children:
        children = [{"type": "p", "uid": _uid(), "children": [{"text": ""}]}]
    return {"type": "doc", "uid": _uid(), "children": children}


def _convert_children(element) -> list[dict]:
    nodes = []
    for child in element.children:
        if isinstance(child, NavigableString):
            text = str(child)
            if text.strip():
                nodes.append({"text": text})
        elif child.name in BLOCK_TAGS:
            nodes.append(_convert_block(child))
        elif child.name == "a":
            nodes.append(_convert_link(child))
        elif child.name == "img":
            nodes.append(_convert_img(child))
        elif child.name == "br":
            nodes.append({"text": "\n"})
        elif child.name in INLINE_MARKS:
            nodes.extend(_convert_inline(child))
        elif child.name == "div" or child.name == "span" or child.name == "section":
            # Unwrap generic containers
            nodes.extend(_convert_children(child))
        else:
            # Fallback: treat as inline text
            nodes.extend(_convert_children(child))
    return nodes


def _convert_block(el) -> dict:
    tag = el.name
    if tag == "pre":
        tag = "code"
    node = {"type": tag, "uid": _uid(), "children": _convert_children(el) or [{"text": ""}]}
    return node


def _convert_link(el) -> dict:
    href = el.get("href", "")
    children = _convert_children(el) or [{"text": el.get_text()}]
    return {
        "type": "a",
        "uid": _uid(),
        "attrs": {"url": href, "target": el.get("target", "_blank")},
        "children": children,
    }


def _convert_img(el) -> dict:
    return {
        "type": "img",
        "uid": _uid(),
        "attrs": {"src": el.get("src", ""), "alt": el.get("alt", "")},
        "children": [{"text": ""}],
    }


def _convert_inline(el) -> list[dict]:
    mark = INLINE_MARKS.get(el.name)
    result = []
    for child in el.children:
        if isinstance(child, NavigableString):
            node = {"text": str(child)}
            if mark:
                node[mark] = True
            result.append(node)
        elif child.name in INLINE_MARKS:
            inner = _convert_inline(child)
            if mark:
                for n in inner:
                    n[mark] = True
            result.extend(inner)
        else:
            result.extend(_convert_children(child))
    return result


# ---------------------------------------------------------------------------
# Value extraction helpers
# ---------------------------------------------------------------------------


def _resolve_file_path(uri: str) -> str | None:
    """Resolve a Drupal file URI (public://...) to an absolute file path."""
    if not DRUPAL_FILES_PATH or not uri:
        return None
    if uri.startswith("public://"):
        rel = uri[len("public://"):]
    elif uri.startswith("private://"):
        return None  # private files not accessible
    else:
        rel = uri
    full = os.path.join(DRUPAL_FILES_PATH, rel)
    return full if os.path.isfile(full) else None


def extract_field_value(
    field_name: str,
    value_cols: list[str],
    rows: list[dict],
    drupal_type: str,
    asset_map: dict[int, str],
    entry_map: dict[str, dict[int, str]],
    field_instances: dict[str, list[str]],
    bundle: str,
) -> object:
    """Extract and convert field values from Drupal rows into ContentStack format."""
    if not rows:
        return None

    is_multi = len(rows) > 1

    values = []
    for row in rows:
        val = _extract_single(field_name, value_cols, row, drupal_type, asset_map, entry_map, field_instances, bundle)
        if val is not None:
            values.append(val)

    if not values:
        return None
    # Reference fields must always be arrays for ContentStack
    if drupal_type in ("entity_reference", "entity_reference_revisions"):
        return values
    return values if is_multi else values[0]


def _extract_single(
    field_name: str,
    value_cols: list[str],
    row: dict,
    drupal_type: str,
    asset_map: dict[int, str],
    entry_map: dict[str, dict[int, str]],
    field_instances: dict[str, list[str]],
    bundle: str,
) -> object:
    # --- File / Image ---
    target_id_col = f"{field_name}_target_id"
    if drupal_type in ("image", "file") or (
        any(c.endswith("_alt") for c in value_cols) and target_id_col in value_cols
    ):
        fid = row.get(target_id_col)
        if fid and fid in asset_map:
            return asset_map[fid]
        return None

    # --- Entity reference ---
    if drupal_type in ("entity_reference", "entity_reference_revisions"):
        target_id = row.get(target_id_col)
        if target_id is None:
            return None
        # Determine target content type
        target_bundles = field_instances.get(field_name, [])
        if target_bundles:
            for tb in target_bundles:
                cs_ct_uid = _sanitize_uid(tb)
                if cs_ct_uid in entry_map and target_id in entry_map[cs_ct_uid]:
                    return {"uid": entry_map[cs_ct_uid][target_id], "_content_type_uid": cs_ct_uid}
        # Fallback: try taxonomy
        for ct_uid, id_map in entry_map.items():
            if target_id in id_map:
                return {"uid": id_map[target_id], "_content_type_uid": ct_uid}
        return None

    # --- Link ---
    if drupal_type == "link":
        uri_col = f"{field_name}_uri"
        title_col = f"{field_name}_title"
        return {
            "title": str(row.get(title_col) or ""),
            "href": str(row.get(uri_col) or ""),
        }

    # --- Rich text ---
    if drupal_type in ("text_long", "text_with_summary"):
        val_col = f"{field_name}_value"
        html = row.get(val_col, "")
        if html:
            return html_to_json_rte(str(html))
        return None

    # --- Formatted text ---
    if drupal_type == "text":
        val_col = f"{field_name}_value"
        return str(row.get(val_col, ""))

    # --- Boolean ---
    if drupal_type == "boolean":
        val_col = f"{field_name}_value"
        v = row.get(val_col)
        return bool(v) if v is not None else None

    # --- Number ---
    if drupal_type in ("integer", "float", "decimal"):
        val_col = f"{field_name}_value"
        v = row.get(val_col)
        if v is not None:
            return float(v) if drupal_type in ("float", "decimal") else int(v)
        return None

    # --- Date ---
    if drupal_type in ("datetime", "timestamp", "created", "changed"):
        val_col = f"{field_name}_value"
        v = row.get(val_col)
        if v is not None:
            return str(v)
        return None

    # --- Default: return the _value column ---
    val_col = f"{field_name}_value"
    if val_col in value_cols:
        v = row.get(val_col)
        return str(v) if v is not None else None

    # If no _value column, return first non-meta column
    for c in value_cols:
        v = row.get(c)
        if v is not None:
            return str(v)
    return None


# ---------------------------------------------------------------------------
# Migration orchestrator
# ---------------------------------------------------------------------------


class DrupalToContentStackMigrator:
    def __init__(self, db_only: bool = False):
        self.db = DB()
        self.api = ContentStackAPI()
        self.db_only = db_only
        # Maps: Drupal entity ID → ContentStack entry UID, keyed by CS content type uid
        self.entry_map: dict[str, dict[int, str]] = {}
        # Maps: Drupal fid → ContentStack asset UID
        self.asset_map: dict[int, str] = {}
        # Existing CS content types to avoid re-creating
        self.existing_ct_uids: set[str] = set()
        # Track all created records for rollback
        self.records: dict = {"assets": [], "entries": [], "content_types": []}

    def run(self):
        log.info("=" * 60)
        log.info("Drupal → ContentStack Migration%s", " (DB only — skipping file assets)" if self.db_only else "")
        log.info("=" * 60)

        self._load_existing_content_types()

        # Phase 1: Assets (files)
        if self.db_only:
            log.info("Skipping Phase 1 (assets) — db-only mode")
        else:
            self._migrate_assets()

        # Phase 2: Taxonomy vocabularies → content types + entries
        self._migrate_taxonomy()

        # Phase 3: Paragraphs → modular block definitions (collected per content type)
        paragraph_types = discover_paragraph_types(self.db)
        paragraph_schemas = self._build_paragraph_schemas(paragraph_types)

        # Phase 4: Node content types → content types + entries
        self._migrate_nodes(paragraph_schemas)

        self._save_records()

        log.info("=" * 60)
        log.info("Migration complete!")
        log.info("  Assets migrated: %d", len(self.asset_map))
        log.info("  Entries migrated: %d", sum(len(v) for v in self.entry_map.values()))
        log.info("  Records saved to: %s", RECORDS_FILE)
        log.info("=" * 60)

        self.db.close()

    # -- helpers -----------------------------------------------------------

    def _load_existing_content_types(self):
        try:
            cts = self.api.get_content_types()
            self.existing_ct_uids = {ct["uid"] for ct in cts}
            log.info("Found %d existing content types in ContentStack", len(self.existing_ct_uids))
        except Exception:
            log.warning("Could not fetch existing content types; will attempt creation for all")

    def _save_records(self):
        """Save all created record UIDs to a JSON file for rollback."""
        with open(RECORDS_FILE, "w") as f:
            json.dump(self.records, f, indent=2)
        log.info("Saved %d asset(s), %d entry/entries, %d content type(s) to %s",
                 len(self.records["assets"]),
                 len(self.records["entries"]),
                 len(self.records["content_types"]),
                 RECORDS_FILE)

    # -- Phase 1: Assets ---------------------------------------------------

    def _migrate_assets(self):
        log.info("-" * 40)
        log.info("Phase 1: Migrating assets (files)")

        has_table = self.db.query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='file_managed'"
        )
        if not has_table:
            log.info("No file_managed table found; skipping assets.")
            return

        files = self.db.query(
            "SELECT fid, uuid, filename, uri, filemime, filesize, status "
            "FROM file_managed WHERE status = 1 ORDER BY fid"
        )
        log.info("Found %d permanent files to migrate", len(files))

        for f in files:
            filepath = _resolve_file_path(f["uri"])
            if not filepath:
                log.debug("Skipping file %s — cannot resolve path: %s", f["fid"], f["uri"])
                continue
            try:
                asset = self.api.upload_asset(filepath, title=f["filename"])
                asset_uid = asset.get("uid", "")
                if asset_uid:
                    self.asset_map[f["fid"]] = asset_uid
                    self.records["assets"].append(asset_uid)
                    log.info("  fid=%d → asset_uid=%s", f["fid"], asset_uid)
            except Exception as exc:
                log.error("  Failed to upload fid=%d (%s): %s", f["fid"], f["filename"], exc)

    # -- Phase 2: Taxonomy -------------------------------------------------

    def _migrate_taxonomy(self):
        log.info("-" * 40)
        log.info("Phase 2: Migrating taxonomy vocabularies")

        vocabs = discover_taxonomy_vocabularies(self.db)
        if not vocabs:
            log.info("No taxonomy vocabularies found.")
            return

        tax_field_tables = discover_field_tables(self.db, "taxonomy_term")
        tax_field_config = discover_field_config(self.db, "taxonomy_term")

        for vocab in vocabs:
            ct_uid = _sanitize_uid(vocab)
            log.info("Vocabulary: %s → content type: %s", vocab, ct_uid)

            # Build schema
            schema = [
                {"display_name": "Title", "uid": "title", "data_type": "text", "mandatory": True, "unique": False,
                 "field_metadata": {"_default": True}, "multiple": False},
            ]
            # Add description field
            schema.append({
                "display_name": "Description", "uid": "description", "data_type": "text",
                "field_metadata": {"multiline": True}, "mandatory": False, "unique": False, "multiple": False,
            })
            # Add custom taxonomy fields
            for tbl, value_cols in tax_field_tables.items():
                field_name = tbl.replace("taxonomy_term__", "")
                # Check if this field is used by this vocabulary
                bundles = self.db.query(f"SELECT DISTINCT bundle FROM `{tbl}` WHERE bundle = %s", (vocab,))
                if not bundles:
                    continue
                cfg = tax_field_config.get(field_name, {})
                drupal_type = cfg.get("type", "string")
                cardinality = cfg.get("cardinality", 1)
                schema.append(map_drupal_field(field_name, drupal_type, value_cols, cardinality))

            # Create content type
            if ct_uid not in self.existing_ct_uids:
                try:
                    self.api.create_content_type({
                        "title": vocab.replace("_", " ").title(),
                        "uid": ct_uid,
                        "description": f"Migrated from Drupal taxonomy vocabulary: {vocab}",
                        "schema": schema,
                    })
                    self.existing_ct_uids.add(ct_uid)
                    self.records["content_types"].append(ct_uid)
                except Exception as exc:
                    if "not unique" in str(exc).lower():
                        log.info("Content type %s already exists, skipping creation.", ct_uid)
                        self.existing_ct_uids.add(ct_uid)
                    else:
                        log.error("Failed to create content type %s: %s", ct_uid, exc)
                        continue

            # Create entries
            terms = self.db.query(
                "SELECT td.tid, tfd.name, tfd.description__value "
                "FROM taxonomy_term_data td "
                "JOIN taxonomy_term_field_data tfd ON td.tid = tfd.tid AND tfd.default_langcode = 1 "
                "WHERE td.vid = %s ORDER BY tfd.weight, td.tid",
                (vocab,),
            )
            self.entry_map.setdefault(ct_uid, {})
            for term in terms:
                entry = {
                    "title": term["name"],
                    "description": term.get("description__value") or "",
                }
                # Add custom field values
                for tbl, value_cols in tax_field_tables.items():
                    field_name = tbl.replace("taxonomy_term__", "")
                    cfg = tax_field_config.get(field_name, {})
                    drupal_type = cfg.get("type", "string")
                    rows = self.db.query(
                        f"SELECT * FROM `{tbl}` WHERE entity_id = %s AND bundle = %s AND deleted = 0 ORDER BY delta",
                        (term["tid"], vocab),
                    )
                    if rows:
                        val = extract_field_value(
                            field_name, value_cols, rows, drupal_type,
                            self.asset_map, self.entry_map, {}, vocab,
                        )
                        if val is not None:
                            entry[_sanitize_uid(field_name)] = val

                try:
                    result = self.api.create_entry(ct_uid, entry)
                    entry_uid = result.get("entry", {}).get("uid", "")
                    if entry_uid:
                        self.entry_map[ct_uid][term["tid"]] = entry_uid
                        self.records["entries"].append({"content_type_uid": ct_uid, "entry_uid": entry_uid})
                except Exception as exc:
                    log.error("Failed to create taxonomy entry tid=%d: %s", term["tid"], exc)

    # -- Phase 3: Paragraph schemas ----------------------------------------

    def _build_paragraph_schemas(self, paragraph_types: list[str]) -> dict[str, dict]:
        """Build ContentStack modular block definitions for each paragraph type."""
        if not paragraph_types:
            return {}

        log.info("-" * 40)
        log.info("Phase 3: Building paragraph/modular-block schemas")

        para_field_tables = discover_field_tables(self.db, "paragraph")
        para_field_config = discover_field_config(self.db, "paragraph")
        schemas = {}

        for ptype in paragraph_types:
            block_uid = _sanitize_uid(ptype)
            log.info("Paragraph type: %s → block: %s", ptype, block_uid)
            block_schema = []

            for tbl, value_cols in para_field_tables.items():
                field_name = tbl.replace("paragraph__", "")
                bundles = self.db.query(f"SELECT DISTINCT bundle FROM `{tbl}` WHERE bundle = %s", (ptype,))
                if not bundles:
                    continue
                cfg = para_field_config.get(field_name, {})
                drupal_type = cfg.get("type", "string")
                cardinality = cfg.get("cardinality", 1)
                field_def = map_drupal_field(field_name, drupal_type, value_cols, cardinality)
                # References inside modular blocks are not supported as "reference" type;
                # convert to text for now
                if field_def["data_type"] == "reference":
                    field_def["data_type"] = "text"
                    field_def.pop("reference_to", None)
                block_schema.append(field_def)

            if not block_schema:
                block_schema.append({
                    "display_name": "Content",
                    "uid": "content",
                    "data_type": "text",
                    "field_metadata": {"multiline": True},
                    "mandatory": False, "unique": False, "multiple": False,
                })

            schemas[ptype] = {
                "title": ptype.replace("_", " ").title(),
                "uid": block_uid,
                "schema": block_schema,
            }

        return schemas

    # -- Phase 4: Nodes ----------------------------------------------------

    def _migrate_nodes(self, paragraph_schemas: dict[str, dict]):
        log.info("-" * 40)
        log.info("Phase 4: Migrating node content types and entries")

        bundles = discover_content_types(self.db)
        field_tables = discover_field_tables(self.db)
        field_config = discover_field_config(self.db)
        field_instances = discover_field_instances(self.db)

        deferred_refs: list[tuple[str, list[dict]]] = []

        for bundle in bundles:
            ct_uid = _sanitize_uid(bundle)
            log.info("Content type: %s → %s", bundle, ct_uid)

            bundle_instances = field_instances.get(bundle, {})

            # Build schema
            schema = [
                {"display_name": "Title", "uid": "title", "data_type": "text", "mandatory": True, "unique": False,
                 "field_metadata": {"_default": True}, "multiple": False},
                {"display_name": "URL", "uid": "url", "data_type": "text", "mandatory": False, "unique": False,
                 "field_metadata": {"_default": True}, "multiple": False},
            ]

            # Track which fields are paragraph reference fields
            paragraph_fields: dict[str, list[str]] = {}  # field_name -> [paragraph_types]

            # Discover fields for this bundle
            for tbl, value_cols in field_tables.items():
                field_name = tbl.replace("node__", "")
                # Check bundle usage
                bundles_using = self.db.query(
                    f"SELECT DISTINCT bundle FROM `{tbl}` WHERE bundle = %s", (bundle,)
                )
                if not bundles_using:
                    continue

                cfg = field_config.get(field_name, {})
                drupal_type = cfg.get("type", "string")
                cardinality = cfg.get("cardinality", 1)

                # Paragraph / entity_reference_revisions → modular blocks
                if drupal_type == "entity_reference_revisions":
                    target_bundles = bundle_instances.get(field_name, [])
                    if target_bundles:
                        # These are paragraph types
                        paragraph_fields[field_name] = target_bundles
                        # Add a modular blocks field
                        blocks = []
                        for pt in target_bundles:
                            if pt in paragraph_schemas:
                                blocks.append(paragraph_schemas[pt])
                        if blocks:
                            schema.append({
                                "display_name": field_name.replace("field_", "").replace("_", " ").title(),
                                "uid": _sanitize_uid(field_name),
                                "data_type": "blocks",
                                "mandatory": False,
                                "unique": False,
                                "multiple": True,
                                "blocks": blocks,
                            })
                        continue

                field_def = map_drupal_field(field_name, drupal_type, value_cols, cardinality)

                # Set reference targets if known
                if field_def["data_type"] == "reference":
                    target_bundles_ref = bundle_instances.get(field_name, [])
                    if target_bundles_ref:
                        field_def["reference_to"] = [_sanitize_uid(tb) for tb in target_bundles_ref]
                    else:
                        # Generic fallback — will accept any
                        field_def["reference_to"] = []
                    field_def["field_metadata"] = {"ref_multiple": field_def.get("multiple", False)}

                schema.append(field_def)

            # Split schema into non-reference and reference fields
            base_schema = [f for f in schema if f["data_type"] != "reference" and "reference_to" not in f]
            ref_fields = [f for f in schema if f["data_type"] == "reference" or "reference_to" in f]

            # Create content type without reference fields first
            if ct_uid not in self.existing_ct_uids:
                try:
                    self.api.create_content_type({
                        "title": bundle.replace("_", " ").title(),
                        "uid": ct_uid,
                        "description": f"Migrated from Drupal content type: {bundle}",
                        "schema": base_schema,
                    })
                    self.existing_ct_uids.add(ct_uid)
                    self.records["content_types"].append(ct_uid)
                except Exception as exc:
                    if "not unique" in str(exc).lower():
                        log.info("Content type %s already exists, skipping creation.", ct_uid)
                        self.existing_ct_uids.add(ct_uid)
                    else:
                        log.error("Failed to create content type %s: %s", ct_uid, exc)
                        continue

            # Queue reference fields to be added after all content types exist
            if ref_fields:
                deferred_refs.append((ct_uid, ref_fields))

            # Create entries
            nodes = self.db.query(
                "SELECT n.nid, nfd.title, nfd.status, nfd.created, nfd.changed, nfd.langcode "
                "FROM node n "
                "JOIN node_field_data nfd ON n.nid = nfd.nid AND nfd.default_langcode = 1 "
                "WHERE n.type = %s ORDER BY n.nid",
                (bundle,),
            )
            log.info("  Found %d entries for %s", len(nodes), bundle)

            self.entry_map.setdefault(ct_uid, {})
            for node in nodes:
                entry: dict = {
                    "title": node["title"] or f"Node {node['nid']}",
                    "url": f"/node/{node['nid']}",
                }

                # Populate field values
                for tbl, value_cols in field_tables.items():
                    field_name = tbl.replace("node__", "")
                    cfg = field_config.get(field_name, {})
                    drupal_type = cfg.get("type", "string")

                    # Skip paragraph fields (handled separately below)
                    if field_name in paragraph_fields:
                        continue

                    rows = self.db.query(
                        f"SELECT * FROM `{tbl}` "
                        f"WHERE entity_id = %s AND bundle = %s AND deleted = 0 ORDER BY delta",
                        (node["nid"], bundle),
                    )
                    if not rows:
                        continue

                    val = extract_field_value(
                        field_name, value_cols, rows, drupal_type,
                        self.asset_map, self.entry_map, bundle_instances, bundle,
                    )
                    if val is not None:
                        entry[_sanitize_uid(field_name)] = val

                # Populate paragraph / modular block fields
                for para_field, para_types in paragraph_fields.items():
                    para_ref_table = f"node__{para_field}"
                    if para_ref_table not in field_tables:
                        continue
                    para_refs = self.db.query(
                        f"SELECT * FROM `{para_ref_table}` "
                        f"WHERE entity_id = %s AND bundle = %s AND deleted = 0 ORDER BY delta",
                        (node["nid"], bundle),
                    )
                    if not para_refs:
                        continue

                    blocks = []
                    para_field_tables = discover_field_tables(self.db, "paragraph")
                    para_field_config = discover_field_config(self.db, "paragraph")

                    for ref_row in para_refs:
                        target_id = ref_row.get(f"{para_field}_target_id")
                        if not target_id:
                            continue
                        # Get paragraph type
                        para_info = self.db.query_one(
                            "SELECT type FROM paragraphs_item WHERE id = %s", (target_id,)
                        )
                        if not para_info:
                            continue
                        ptype = para_info["type"]
                        block_uid = _sanitize_uid(ptype)

                        # Build block data
                        block_data = {}
                        for ptbl, pval_cols in para_field_tables.items():
                            pfield_name = ptbl.replace("paragraph__", "")
                            prows = self.db.query(
                                f"SELECT * FROM `{ptbl}` "
                                f"WHERE entity_id = %s AND bundle = %s AND deleted = 0 ORDER BY delta",
                                (target_id, ptype),
                            )
                            if not prows:
                                continue
                            pcfg = para_field_config.get(pfield_name, {})
                            pdrupal_type = pcfg.get("type", "string")
                            # Inside blocks, references aren't supported — convert to text
                            if pdrupal_type in ("entity_reference", "entity_reference_revisions"):
                                pdrupal_type = "string"
                            pval = extract_field_value(
                                pfield_name, pval_cols, prows, pdrupal_type,
                                self.asset_map, self.entry_map, {}, ptype,
                            )
                            if pval is not None:
                                block_data[_sanitize_uid(pfield_name)] = pval

                        if block_data:
                            blocks.append({block_uid: block_data})

                    if blocks:
                        entry[_sanitize_uid(para_field)] = blocks

                # Create entry
                try:
                    result = self.api.create_entry(ct_uid, entry)
                    entry_uid = result.get("entry", {}).get("uid", "")
                    if entry_uid:
                        self.entry_map[ct_uid][node["nid"]] = entry_uid
                        self.records["entries"].append({"content_type_uid": ct_uid, "entry_uid": entry_uid})
                except Exception as exc:
                    log.error("  Failed to create entry nid=%d: %s", node["nid"], exc)

        # Second pass: add deferred reference fields now that all content types exist
        for ct_uid, ref_fields in deferred_refs:
            try:
                self.api.update_content_type(ct_uid, {"schema": ref_fields})
            except Exception as exc:
                log.error("Failed to add reference fields to %s: %s", ct_uid, exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Drupal → ContentStack Migration")
    parser.add_argument(
        "--db-only",
        action="store_true",
        help="Migrate only database contents (taxonomy, paragraphs, nodes) without uploading file assets from DRUPAL_FILES_PATH",
    )
    args = parser.parse_args()

    # Validate config
    missing = []
    required_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "CONTENTSTACK_API_KEY", "CONTENTSTACK_MANAGEMENT_TOKEN"]
    for var in required_vars:
        val = os.getenv(var)
        if not val or val == "UNKNOWN":
            missing.append(var)
    if missing:
        log.error("The following environment variables must be set in .env:\n  %s", "\n  ".join(missing))
        sys.exit(1)

    migrator = DrupalToContentStackMigrator(db_only=args.db_only)
    migrator.run()


if __name__ == "__main__":
    main()
