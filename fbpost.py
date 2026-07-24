"""
fbpost.py — MULTI-PAGE / MULTI-JOB VERSION (GitHub Actions matrix, positional — no assignment)
──────────────────────────────────────────────────────────────
Runs several Facebook Pages in parallel — each one handled by its OWN
GitHub Actions matrix job (own runner, own process), not by asyncio
threads/semaphores inside one process.

OWNERSHIP MODEL (NEW — no AssignedRepo/AssignedStatus/AssignedAt at all):
Each matrix job is given a plain numeric JOB_INDEX (0, 1, 2, ...) by the
workflow. At the start of every run, the script reads the "Pages" tab,
filters to Active rows (in sheet row order), and this job simply takes
the page sitting at position JOB_INDEX in that list — nothing is written
back to "claim" it, nothing is locked, and nothing needs a TTL/heartbeat
column to expire. Because the Pages tab's row order barely changes between
runs, the SAME job index keeps landing on the SAME page run after run.
If you insert/remove/reorder page rows, indices shift accordingly — that's
expected and is the trade-off for not needing any ownership bookkeeping.

How many jobs run is controlled by the workflow's matrix, which is built
from the number of Active pages found in the sheet (see --count-pages
below) capped by an optional job_count input. Leave job_count blank to
run ALL active pages; set it to cap how many pages get worked this cycle.

Run modes:
    python -u fbpost.py --setup-sheet     # create/repair tabs+headers, then exit
    python -u fbpost.py --count-pages     # print how many Active pages exist (used by
                                            # the workflow to size its job matrix), then exit
    python -u fbpost.py --once            # one post for this job's page, then exit
    python -u fbpost.py                   # long-running: loops this job's page on its
                                            # own interval until MaxRuntimeMinutes

Only the job with JOB_INDEX == "0" is responsible for re-dispatching the
NEXT scheduled workflow run (see IS_REQUEUE_LEADER below) — otherwise
every parallel matrix job would each independently fire a new workflow
run and you'd get many duplicate runs queued every cycle.

═══════════════════════════════════════════════════════════════════════
GOOGLE SHEET LAYOUT  (spreadsheet id: CAPTIONS_SHEET_ID env var — set this
via a GitHub secret, see the workflow). Tabs/headers are created/repaired
AUTOMATICALLY on every run — you never need to run --setup-sheet by hand.
═══════════════════════════════════════════════════════════════════════

Tab "Settings" — MASTER defaults, two columns, one row per setting:
    Key                     Value
    LoopIntervalMinutes     60         # default minutes between posts, per page
    MaxRuntimeMinutes       300        # default total runtime before exiting
    MegaFolder              fbreels
    MegaMoveFolder          fbreels_uploaded
    Link_Percentage         100
    UrlReplaceCount         1
    UrlReplaceMode          unique
    UrlReplaceEnabled       TRUE
    PostMode                rotation   # "rotation" or "queue" — see Pages.PostMode below
    HeartbeatMinutes        15         # how often FbStorageState is refreshed back to the
                                       # sheet while a page is actively running

Tab "Pages" — one row PER FACEBOOK PAGE. Every column except PageId and
FbStorageState is an OVERRIDE: leave it blank to fall back to Settings.
Which job index gets which row is purely POSITIONAL (row order among
Active rows) — there is no assignment/lock column any more.
    PageId               REQUIRED, short unique id, e.g. "page1" (used in logs)
    PageName             the page's display name. You can type this in yourself,
                          or leave it blank/stale — the script automatically
                          scrapes the real name from Facebook (via PageActualId)
                          and keeps this cell in sync on every run.
    PageActualId          REQUIRED for name-sync — the numeric Facebook page id
                          from the page's URL, e.g. for
                          https://web.facebook.com/profile.php?id=61587108955798
                          this is 61587108955798
    Status               Active | Paused  (Paused rows are never picked, and are
                          skipped when numbering positions for JOB_INDEX)
    MegaFolder           override
    MegaMoveFolder       override
    Caption              override — the WITH-link caption for "rotation" mode
    WithoutLinkCap        override — the WITHOUT-link caption for "rotation" mode
    Link_Percentage      override
    LoopIntervalMinutes  override — base minutes between posts (used as the center point
                          for auto-randomization if Min/Max below are left blank)
    LoopIntervalMinMinutes override — explicit floor for the random per-cycle wait (optional)
    LoopIntervalMaxMinutes override — explicit ceiling for the random per-cycle wait (optional).
                          If both Min/Max are blank, the script auto-derives a ±25% random
                          range around LoopIntervalMinutes, so every cycle waits a
                          different, human-like amount of time instead of one fixed interval.
    UrlReplaceCount      override
    UrlReplaceMode       override
    UrlReplaceEnabled    override
    PostMode             override: "rotation" (old behavior) or "queue" (new — see PostQueue tab)
    FbStorageState       Playwright storage_state JSON for THIS page's login session.
                          Seed it once (paste your session JSON); the script refreshes
                          it here periodically, so it never goes stale.
    LastPostedFile       written by the script — filename of the last successful post
    Notes                free text, ignored by the script

Tab "Urls" — shared (or per-page) pool of URLs to rotate into "rotation"
mode captions.
    Urls        one URL per row
    Status      blank = unused; script writes "Posted"/"Rejected" after use
    PageId      OPTIONAL. Blank = usable by any page.

Tab "PostQueue" — ONLY needed for pages with PostMode = "queue".
    FileName    exact Mega filename this row is for, e.g. "clip_014.mp4"
    Caption     the caption to use for this exact file
    Hashtags    optional, appended to the caption on its own line
    PageId      OPTIONAL — restrict this row to one page; blank = any page
    Status      blank = pending; script writes "Posted" after a successful post
"""

import asyncio, json, os, random, re, subprocess, sys, tempfile, time
import urllib.request, urllib.error
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
import functools

print = functools.partial(print, flush=True)

try:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    HAS_GOOGLE = True
except ImportError:
    HAS_GOOGLE = False
    print("⚠️  Google API libraries not installed")

from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
FB_STORAGE_STATE_ENV      = "FB_STORAGE_STATE"      # one-time seed fallback only
GOOGLE_CREDS_ENV          = "GOOGLE_CREDENTIALS_JSON"
CAPTIONS_SHEET_ID_ENV     = "CAPTIONS_SHEET_ID"     # REQUIRED — set via a GitHub secret,
                                                     # no hardcoded fallback any more

# How long before this job's own MaxRuntimeMinutes window closes should we
# fire the self-requeue call, so the next job is already queued and starts
# essentially back-to-back with this one ending.
SELF_REQUEUE_LEAD_SECONDS = 60

_REQUEUED = False  # module-level guard so we only ever dispatch once per run

# Which matrix job-slot this process is (workflow sets JOB_INDEX per matrix
# entry, e.g. "0", "1", "2", ...). Defaults to "0" for non-matrix / single
# job runs so nothing breaks if it's not set. This is a plain POSITION into
# the list of Active pages — not an identity that owns anything in the sheet.
JOB_INDEX = os.environ.get("JOB_INDEX", "0").strip() or "0"
# How many jobs are running THIS cycle (informational/logging only now).
JOB_COUNT = os.environ.get("JOB_COUNT", "1").strip() or "1"
# The original user-set cap (workflow's job_count input) — may be blank,
# meaning "no limit / run all pages". Re-sent unchanged on self-requeue so
# the same limit (or lack of one) applies to the next scheduled cycle.
JOB_LIMIT = os.environ.get("JOB_LIMIT", "").strip()

# Only job-slot 0 dispatches the next scheduled workflow run — otherwise
# every parallel matrix job would each fire its own duplicate re-dispatch.
IS_REQUEUE_LEADER = (JOB_INDEX == "0")

VIDEO_EXTENSIONS  = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
MEGA_REMOTE_NAME  = "mega"
URL_REGEX         = re.compile(r'https?://\S+')
SHEETS_CELL_CHAR_LIMIT = 50000

SETTINGS_TAB   = "Settings"
PAGES_TAB      = "Pages"
URLS_TAB       = "Urls"
POSTQUEUE_TAB  = "PostQueue"

SETTINGS_DEFAULTS = {
    "loopintervalminutes":    "60",
    "loopintervalminminutes": "",     # optional explicit override — blank = auto-derive ±25% of LoopIntervalMinutes
    "loopintervalmaxminutes": "",     # optional explicit override — blank = auto-derive ±25% of LoopIntervalMinutes
    "maxruntimeminutes":      "300",
    "megafolder":             "fbreels",
    "megamovefolder":         "fbreels_uploaded",
    "link_percentage":        "100",
    "urlreplacecount":        "1",
    "urlreplacemode":         "unique",
    "urlreplaceenabled":      "TRUE",
    "postmode":               "rotation",
    "heartbeatminutes":       "15",     # how often FbStorageState gets refreshed back to the
                                        # sheet while a page's job is actively running
    "linkrejectmaxretries":   "2",
    "urlswaponrejectonly":    "FALSE",
    "mindelayseconds":        "2.5",   # random human-like pause floor between actions/steps
    "maxdelayseconds":        "7",     # random human-like pause ceiling between actions/steps
}

PAGES_HEADERS = [
    "PageId", "PageName", "PageActualId", "Status", "MegaFolder", "MegaMoveFolder", "Caption",
    "WithoutLinkCap", "Link_Percentage", "LoopIntervalMinutes", "LoopIntervalMinMinutes",
    "LoopIntervalMaxMinutes", "UrlReplaceCount",
    "UrlReplaceMode", "UrlReplaceEnabled", "UrlSwapOnRejectOnly", "PostMode", "FbStorageState",
    "LastPostedFile", "Notes",
]
SETTINGS_HEADERS  = ["Key", "Value"]
URLS_HEADERS      = ["Urls", "Status", "PageId"]
POSTQUEUE_HEADERS = ["FileName", "Caption", "Hashtags", "PageId", "Status"]

STORAGE_STATE_DIR = Path("storage_states")
SCREENSHOTS_DIR    = Path("screenshots")

# ─────────────────────────────────────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────────────────────────────────────
def log(page_id, msg, kind="info"):
    icon = {"info": "ℹ️ ", "ok": "✅", "warn": "⚠️ ", "fail": "❌", "step": "▶️ "}.get(kind, "ℹ️ ")
    tag = f"[{page_id}]" if page_id else "[main]"
    print(f"{icon} {tag} {msg}")

def step(page_id, msg):  log(page_id, msg, "step")
def info(page_id, msg):  log(page_id, msg, "info")
def ok(page_id, msg):    log(page_id, msg, "ok")
def warn(page_id, msg):  log(page_id, msg, "warn")
def fail(page_id, msg):  log(page_id, msg, "fail")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def minutes_since(iso_ts: str) -> float:
    try:
        then = datetime.fromisoformat(iso_ts)
        if then.tzinfo is None:
            then = then.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - then).total_seconds() / 60.0
    except Exception:
        return 10 ** 9   # unparsable / empty -> treat as "ancient", i.e. free


def _to_int(val, default):
    try:
        return int(str(val).strip())
    except (TypeError, ValueError):
        return default


def _to_bool(val, default):
    if val is None or str(val).strip() == "":
        return default
    return str(val).strip().lower() in ("true", "yes", "y", "1", "on", "enable", "enabled")


def _to_float(val, default):
    try:
        return float(str(val).strip())
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS — thin generic layer (tab-aware)
# ─────────────────────────────────────────────────────────────────────────────

def build_google_creds():
    if not HAS_GOOGLE:
        raise RuntimeError("google-auth libraries not installed")
    creds_json = os.environ.get(GOOGLE_CREDS_ENV)
    if not creds_json:
        raise RuntimeError(f"Missing {GOOGLE_CREDS_ENV}")
    data = json.loads(creds_json)
    creds = Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes", ["https://www.googleapis.com/auth/spreadsheets"]),
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return creds


def build_sheets_service(creds):
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


class Sheet:
    """Small wrapper around the Sheets v4 API for one spreadsheet."""

    def __init__(self, service, spreadsheet_id):
        self.svc = service
        self.id = spreadsheet_id
        self._tab_cache = None

    @staticmethod
    def _retry(fn, *, attempts=3, base_delay=1.5, page_id=None, what="Sheets call"):
        last_err = None
        for i in range(1, attempts + 1):
            try:
                return fn()
            except Exception as e:
                last_err = e
                if i < attempts:
                    warn(page_id, f"{what} failed (attempt {i}/{attempts}): {e} — retrying")
                    time.sleep(base_delay * i)
        warn(page_id, f"{what} failed after {attempts} attempts: {last_err}")
        raise last_err

    def existing_tabs(self, refresh=False):
        if self._tab_cache is None or refresh:
            meta = self._retry(
                lambda: self.svc.spreadsheets().get(spreadsheetId=self.id).execute(),
                what="Sheets metadata read")
            self._tab_cache = {s["properties"]["title"] for s in meta.get("sheets", [])}
        return self._tab_cache

    def ensure_tab(self, title: str, headers: list[str]):
        if title not in self.existing_tabs():
            self._retry(lambda: self.svc.spreadsheets().batchUpdate(
                spreadsheetId=self.id,
                body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
            ).execute(), what=f"create tab '{title}'")
            self._tab_cache = None
            ok(None, f"Created missing tab '{title}'")
        rows = self.read_rows(title, "A1:Z1")
        if not rows or not any(c.strip() for c in rows[0]):
            self._retry(lambda: self.svc.spreadsheets().values().update(
                spreadsheetId=self.id, range=f"'{title}'!A1",
                valueInputOption="RAW", body={"values": [headers]},
            ).execute(), what=f"write header row for '{title}'")
            ok(None, f"Wrote header row for '{title}': {headers}")
        else:
            info(None, f"Tab '{title}' already has a header row — leaving it as-is")

    def ensure_columns(self, title: str, required_headers: list[str]):
        if title not in self.existing_tabs():
            return
        rows = self.read_rows(title, "A1:Z1")
        header = rows[0] if rows else []
        present = {h.strip().lower() for h in header if h.strip()}
        missing = [h for h in required_headers if h.lower() not in present]
        if not missing:
            return
        next_col = len(header)
        for i, h in enumerate(missing):
            col_letter = self._col_letter(next_col + i)
            self._retry(lambda cl=col_letter, hh=h: self.svc.spreadsheets().values().update(
                spreadsheetId=self.id, range=f"'{title}'!{cl}1",
                valueInputOption="RAW", body={"values": [[hh]]},
            ).execute(), what=f"add column '{h}' to '{title}'")
        ok(None, f"Added missing column(s) to '{title}': {missing}")

    def read_rows(self, tab: str, a1_range: str = "A:Z") -> list[list[str]]:
        try:
            result = self._retry(lambda: self.svc.spreadsheets().values().get(
                spreadsheetId=self.id, range=f"'{tab}'!{a1_range}"
            ).execute(), what=f"read '{tab}'!{a1_range}")
        except Exception as e:
            warn(None, f"Sheets read failed ('{tab}'!{a1_range}) after retries: {e}")
            return []
        return result.get("values", [])

    def write_cell(self, tab: str, row_num: int, col_idx: int, value: str):
        col_letter = self._col_letter(col_idx)
        try:
            self._retry(lambda: self.svc.spreadsheets().values().update(
                spreadsheetId=self.id, range=f"'{tab}'!{col_letter}{row_num}",
                valueInputOption="RAW", body={"values": [[value]]},
            ).execute(), what=f"write '{tab}'!{col_letter}{row_num}")
            return True
        except Exception as e:
            warn(None, f"Write failed '{tab}'!{col_letter}{row_num}' after retries: {e}")
            return False

    def clear_cell(self, tab: str, row_num: int, col_idx: int):
        col_letter = self._col_letter(col_idx)
        try:
            self._retry(lambda: self.svc.spreadsheets().values().clear(
                spreadsheetId=self.id, range=f"'{tab}'!{col_letter}{row_num}", body={}
            ).execute(), what=f"clear '{tab}'!{col_letter}{row_num}")
        except Exception as e:
            warn(None, f"Clear failed '{tab}'!{col_letter}{row_num}' after retries: {e}")

    @staticmethod
    def _col_letter(idx: int) -> str:
        letters = ""
        idx += 1
        while idx > 0:
            idx, rem = divmod(idx - 1, 26)
            letters = chr(65 + rem) + letters
        return letters

    def as_dicts(self, tab: str):
        rows = self.read_rows(tab)
        if not rows:
            return [], {}
        header = [h.strip() for h in rows[0]]
        col_index = {h.lower(): i for i, h in enumerate(header) if h}
        out = []
        for row_num, row in enumerate(rows[1:], start=2):
            d = {header[i]: (row[i] if i < len(row) else "").strip()
                 for i in range(len(header)) if header[i]}
            d["_row"] = row_num
            out.append(d)
        return out, col_index


def setup_sheet(sheets_service, spreadsheet_id):
    """Idempotent — creates missing tabs/headers/columns/default settings.
    Never touches existing data. Called AUTOMATICALLY at the start of every
    run now (not just via --setup-sheet), so a brand-new / blank sheet ID
    just works the first time it's used instead of erroring out."""
    step(None, f"Verifying sheet structure on spreadsheet {spreadsheet_id}")
    sh = Sheet(sheets_service, spreadsheet_id)
    sh.ensure_tab(SETTINGS_TAB, SETTINGS_HEADERS)
    sh.ensure_tab(PAGES_TAB, PAGES_HEADERS)
    sh.ensure_tab(URLS_TAB, URLS_HEADERS)
    sh.ensure_tab(POSTQUEUE_TAB, POSTQUEUE_HEADERS)
    sh.ensure_columns(PAGES_TAB, ["PageActualId", "LoopIntervalMinMinutes", "LoopIntervalMaxMinutes"])

    settings_rows, _ = sh.as_dicts(SETTINGS_TAB)
    present_keys = {r.get("Key", "").strip().lower() for r in settings_rows}
    existing = sh.read_rows(SETTINGS_TAB)
    next_row = len(existing) + 1 if existing else 2
    appended = [[key, val] for key, val in SETTINGS_DEFAULTS.items() if key not in present_keys]
    if appended:
        sh.svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{SETTINGS_TAB}'!A{next_row}",
            valueInputOption="RAW",
            body={"values": appended},
        ).execute()
        ok(None, f"Seeded {len(appended)} default setting(s) into '{SETTINGS_TAB}'")

    pages_rows, _ = sh.as_dicts(PAGES_TAB)
    if not pages_rows:
        warn(None, f"'{PAGES_TAB}' has no page rows yet — add one row per Facebook "
                    f"Page (PageId + FbStorageState are the minimum required fields)")
    ok(None, "Sheet structure OK")
    return sh


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS — master defaults + per-page overrides
# ─────────────────────────────────────────────────────────────────────────────

def load_master_settings(sh: Sheet) -> dict:
    rows, _ = sh.as_dicts(SETTINGS_TAB)
    settings = dict(SETTINGS_DEFAULTS)
    for r in rows:
        k = r.get("Key", "").strip().lower()
        v = r.get("Value", "").strip()
        if k and v:
            settings[k] = v
    return settings


def effective(page_row: dict, master: dict, key: str, column: str):
    val = page_row.get(column, "").strip()
    return val if val else master.get(key.lower(), "")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PageConfig:
    page_id: str
    page_name: str
    page_actual_id: str
    row_num: int
    mega_folder: str
    mega_move_folder: str
    caption: str
    without_link_caption: str
    link_percentage: int
    loop_interval_minutes: int
    loop_interval_min_minutes: int
    loop_interval_max_minutes: int
    url_replace_count: int
    url_replace_mode: str
    url_replace_enabled: bool
    url_swap_on_reject_only: bool
    post_mode: str
    storage_state_json: str | None
    max_runtime_minutes: int
    heartbeat_minutes: int
    link_reject_max_retries: int
    delay_min_seconds: float
    delay_max_seconds: float


def _derive_loop_interval_range(row: dict, master: dict) -> tuple[int, int]:
    """Returns (min_minutes, max_minutes) for the randomized per-cycle wait.
    If LoopIntervalMinMinutes/MaxMinutes are explicitly set (page override or
    Settings), those are used as-is. Otherwise, auto-derives a ±25% spread
    around the base LoopIntervalMinutes value, so posting always waits a
    different, human-like amount of time each cycle instead of one fixed
    interval — no extra sheet configuration required."""
    base = _to_int(effective(row, master, "loopintervalminutes", "LoopIntervalMinutes"), 60)
    min_raw = effective(row, master, "loopintervalminminutes", "LoopIntervalMinMinutes").strip()
    max_raw = effective(row, master, "loopintervalmaxminutes", "LoopIntervalMaxMinutes").strip()
    if min_raw or max_raw:
        lo = _to_int(min_raw, base)
        hi = _to_int(max_raw, base)
    else:
        spread = max(1, round(base * 0.25))
        lo = max(1, base - spread)
        hi = base + spread
    if hi < lo:
        lo, hi = hi, lo
    return lo, hi


def build_page_config(row: dict, master: dict, max_runtime_minutes: int) -> PageConfig:
    loop_min, loop_max = _derive_loop_interval_range(row, master)
    return PageConfig(
        page_id=row["PageId"],
        page_name=row.get("PageName") or row["PageId"],
        page_actual_id=row.get("PageActualId", "").strip(),
        row_num=row["_row"],
        mega_folder=effective(row, master, "megafolder", "MegaFolder"),
        mega_move_folder=effective(row, master, "megamovefolder", "MegaMoveFolder"),
        caption=row.get("Caption", ""),
        without_link_caption=row.get("WithoutLinkCap", ""),
        link_percentage=max(0, min(100, _to_int(effective(row, master, "link_percentage", "Link_Percentage"), 100))),
        loop_interval_minutes=_to_int(effective(row, master, "loopintervalminutes", "LoopIntervalMinutes"), 60),
        loop_interval_min_minutes=loop_min,
        loop_interval_max_minutes=loop_max,
        url_replace_count=_to_int(effective(row, master, "urlreplacecount", "UrlReplaceCount"), 1),
        url_replace_mode=(effective(row, master, "urlreplacemode", "UrlReplaceMode") or "unique").lower(),
        url_replace_enabled=_to_bool(effective(row, master, "urlreplaceenabled", "UrlReplaceEnabled"), True),
        url_swap_on_reject_only=_to_bool(effective(row, master, "urlswaponrejectonly", "UrlSwapOnRejectOnly"), False),
        post_mode=(effective(row, master, "postmode", "PostMode") or "rotation").lower(),
        storage_state_json=row.get("FbStorageState") or None,
        max_runtime_minutes=max_runtime_minutes,
        heartbeat_minutes=_to_int(master.get("heartbeatminutes"), 15),
        link_reject_max_retries=_to_int(master.get("linkrejectmaxretries"), 2),
        delay_min_seconds=max(0.3, _to_float(master.get("mindelayseconds"), 2.5)),
        delay_max_seconds=max(0.6, _to_float(master.get("maxdelayseconds"), 7)),
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE SELECTION — purely positional now, no assignment/lock columns at all
# ─────────────────────────────────────────────────────────────────────────────

def get_active_pages(sh: Sheet) -> list[dict]:
    """Every non-Paused row in the Pages tab, in sheet row order. This list's
    order is what JOB_INDEX indexes into — position N gets matrix job N."""
    rows, _ = sh.as_dicts(PAGES_TAB)
    return [r for r in rows
            if r.get("PageId", "").strip()
            and (r.get("Status", "") or "Active").strip().lower() != "paused"]


def pick_page_for_job(active_pages: list[dict], job_index: str) -> dict | None:
    try:
        idx = int(job_index)
    except (TypeError, ValueError):
        idx = 0
    if idx < 0 or idx >= len(active_pages):
        return None
    return active_pages[idx]


def get_pages_col_index(sh: Sheet) -> dict:
    header = sh.read_rows(PAGES_TAB, "A1:Z1")
    if not header:
        return {}
    return {h.strip().lower(): i for i, h in enumerate(header[0]) if h.strip()}


# ─────────────────────────────────────────────────────────────────────────────
# SELF-REQUEUE — dispatch the next workflow run from inside Python, early
# ─────────────────────────────────────────────────────────────────────────────

def trigger_self_requeue():
    """Fires a workflow_dispatch for the NEXT run via the GitHub API, from
    inside this Python process — well before the job's other steps
    (artifact upload etc.) would otherwise get to it. Uses GH_PAT if set
    (needed for cross-repo dispatch), otherwise falls back to the
    auto-provided GITHUB_TOKEN (works fine for same-repo dispatch as long
    as the workflow grants `permissions: actions: write` — no long-lived
    PAT secret required for the common case). Also needs GITHUB_REPOSITORY,
    GITHUB_REF_NAME (both standard/auto in Actions) and GITHUB_WORKFLOW_FILE
    (set explicitly in the workflow YAML env, since the built-in
    GITHUB_WORKFLOW is the display name, not the filename the API needs).

    The Google Sheet ID is intentionally NOT included in the dispatch
    payload — it stays out of the Actions run history entirely and is
    resolved fresh from the CAPTIONS_SHEET_ID secret on the next run.

    Only the job-slot 0 process ever calls this (see IS_REQUEUE_LEADER) —
    every other matrix job-slot skips it so the workflow isn't re-dispatched
    multiple times per cycle. JOB_LIMIT (the original job_count input, or
    blank for "no limit") is re-sent so the same cap applies next cycle."""
    global _REQUEUED
    if _REQUEUED:
        return
    if not IS_REQUEUE_LEADER:
        return
    token       = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")
    repo        = os.environ.get("GITHUB_REPOSITORY")
    workflow    = os.environ.get("GITHUB_WORKFLOW_FILE")
    ref         = os.environ.get("GITHUB_REF_NAME")
    if not all([token, repo, workflow, ref]):
        warn(None, "Self-requeue from Python skipped (missing token / GITHUB_WORKFLOW_FILE "
                    "/ GITHUB_REF_NAME) — the workflow's own fallback step will handle it")
        return

    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
    payload = json.dumps({
        "ref": ref,
        "inputs": {
            "job_count": JOB_LIMIT,
        },
    }).encode()
    req = urllib.request.Request(url, data=payload, method="POST", headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "fb-reel-self-requeue",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            _REQUEUED = True
            ok(None, f"Next run queued early (HTTP {resp.status}, job_count={JOB_LIMIT or '(no limit)'}) — "
                      f"will start back-to-back with this one")
            try:
                Path(".requeued").write_text(now_iso(), encoding="utf-8")
            except Exception:
                pass
    except urllib.error.HTTPError as e:
        warn(None, f"Self-requeue API call failed: HTTP {e.code} {e.read()[:300]}")
    except Exception as e:
        warn(None, f"Self-requeue API call failed: {e}")


async def schedule_self_requeue(max_runtime_minutes: int):
    """Background task: sleeps until SELF_REQUEUE_LEAD_SECONDS before this
    job's own MaxRuntimeMinutes window would close, then fires the next
    run. Cancelled cleanly if the job finishes earlier on its own (in which
    case main_async() calls trigger_self_requeue() itself, immediately).
    No-op on any job-slot other than 0 — see IS_REQUEUE_LEADER."""
    if not IS_REQUEUE_LEADER:
        return
    wait = max(0, max_runtime_minutes * 60 - SELF_REQUEUE_LEAD_SECONDS)
    try:
        await asyncio.sleep(wait)
        info(None, f"~{SELF_REQUEUE_LEAD_SECONDS}s left in this job's window — requeuing next run now")
        trigger_self_requeue()
    except asyncio.CancelledError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# MEGA.NZ (via rclone)
# ─────────────────────────────────────────────────────────────────────────────

def _run_rclone(page_id, args: list[str], timeout: int = 300, quiet_substrings: tuple[str, ...] = ()):
    cmd = ["rclone"] + args
    info(page_id, f"rclone {' '.join(args)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        fail(page_id, "rclone not installed / not on PATH")
        raise
    except subprocess.TimeoutExpired:
        fail(page_id, f"rclone timed out after {timeout}s")
        raise
    if result.returncode != 0:
        stderr_lower = (result.stderr or "").lower()
        if quiet_substrings and any(s.lower() in stderr_lower for s in quiet_substrings):
            info(page_id, f"rclone exit {result.returncode} (expected): {result.stderr.strip()[:200]}")
        else:
            warn(page_id, f"rclone exit {result.returncode}: {result.stderr.strip()[:400]}")
    return result.returncode, result.stdout, result.stderr


def mega_list_videos(page_id, folder: str, missing_dir_ok: bool = False) -> list[dict]:
    remote_path = f"{MEGA_REMOTE_NAME}:{folder}"
    quiet = ("directory not found",) if missing_dir_ok else ()
    rc, out, err = _run_rclone(page_id, ["lsjson", remote_path, "--files-only"], quiet_substrings=quiet)
    if rc != 0:
        if missing_dir_ok and "directory not found" in (err or "").lower():
            info(page_id, f"No claim folder yet at '{folder}' — nothing currently claimed there")
            return []
        raise RuntimeError(f"rclone lsjson failed: {err.strip()[:300]}")
    entries = json.loads(out) if out.strip() else []
    videos = [e for e in entries if Path(e.get("Name", "")).suffix.lower() in VIDEO_EXTENSIONS]
    videos.sort(key=lambda e: e.get("ModTime", ""))
    info(page_id, f"Found {len(videos)} video(s) in {folder}")
    return videos


def mega_download_video(page_id, folder: str, file_name: str, dest_dir: str) -> str:
    remote_path = f"{MEGA_REMOTE_NAME}:{folder}/{file_name}"
    dest_path = os.path.join(dest_dir, file_name)
    rc, out, err = _run_rclone(page_id, ["copyto", remote_path, dest_path, "--progress"], timeout=1800)
    if rc != 0 or not os.path.exists(dest_path):
        raise RuntimeError(f"rclone download failed: {err.strip()[:300]}")
    ok(page_id, f"Downloaded {file_name} ({os.path.getsize(dest_path)//(1024*1024)} MB)")
    return dest_path


def mega_move_to_uploaded(page_id, src_folder: str, dst_folder: str, file_name: str):
    src = f"{MEGA_REMOTE_NAME}:{src_folder}/{file_name}"
    dst = f"{MEGA_REMOTE_NAME}:{dst_folder}/{file_name}"
    rc, out, err = _run_rclone(page_id, ["moveto", src, dst])
    if rc != 0:
        raise RuntimeError(f"rclone move failed: {err.strip()[:300]}")
    ok(page_id, "Moved to uploaded folder")


def mega_claim_folder(mega_folder: str, page_id: str) -> str:
    return f"{mega_folder}/_claimed_{page_id}"


def mega_try_claim(page_id, src_folder: str, file_name: str, claim_folder: str) -> bool:
    src = f"{MEGA_REMOTE_NAME}:{src_folder}/{file_name}"
    dst = f"{MEGA_REMOTE_NAME}:{claim_folder}/{file_name}"
    rc, out, err = _run_rclone(page_id, ["moveto", src, dst])
    return rc == 0


def mega_return_to_pool(page_id, claim_folder: str, mega_folder: str, file_name: str) -> bool:
    src = f"{MEGA_REMOTE_NAME}:{claim_folder}/{file_name}"
    dst = f"{MEGA_REMOTE_NAME}:{mega_folder}/{file_name}"
    rc, out, err = _run_rclone(page_id, ["moveto", src, dst])
    if rc != 0:
        warn(page_id, f"Could not return '{file_name}' to the pool: {err.strip()[:200]}")
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# URLS TAB
# ─────────────────────────────────────────────────────────────────────────────

URLS_POSTED_VALUES = {"posted", "replaced", "rejected"}

def urls_get_next(sh: Sheet, page_id: str, count: int):
    rows, col_index = sh.as_dicts(URLS_TAB)
    if "urls" not in col_index:
        fail(page_id, f"No 'Urls' column in '{URLS_TAB}' tab")
        return [], [], None
    status_col = col_index.get("status")
    found_urls, found_rows = [], []
    for r in rows:
        if len(found_urls) >= count:
            break
        url = r.get("Urls", "").strip()
        if not url:
            continue
        row_page = r.get("PageId", "").strip()
        if row_page and row_page != page_id:
            continue
        if r.get("Status", "").strip().lower() in URLS_POSTED_VALUES:
            continue
        found_urls.append(url)
        found_rows.append(r["_row"])
    return found_urls, found_rows, status_col


def urls_mark_posted(sh: Sheet, rows: list[int], status_col_idx, status_value="Posted"):
    if not rows or status_col_idx is None:
        return
    for row_num in rows:
        sh.write_cell(URLS_TAB, row_num, status_col_idx, status_value)


def replace_urls_in_caption(caption: str, new_urls: list[str], mode: str, replace_count: int) -> str:
    if not new_urls:
        return caption
    matches = list(URL_REGEX.finditer(caption))
    if not matches:
        if mode == "same":
            return f"{caption}\n{new_urls[0]}"
        return caption + "\n" + "\n".join(new_urls[:replace_count])
    n = min(replace_count, len(matches))
    pieces, last_end = [], 0
    for i, m in enumerate(matches):
        pieces.append(caption[last_end:m.start()])
        if i < n:
            pieces.append(new_urls[0] if mode == "same" else (new_urls[i] if i < len(new_urls) else m.group(0)))
        else:
            pieces.append(m.group(0))
        last_end = m.end()
    pieces.append(caption[last_end:])
    return "".join(pieces)


# ─────────────────────────────────────────────────────────────────────────────
# POSTQUEUE TAB
# ─────────────────────────────────────────────────────────────────────────────

def postqueue_find_for_file(sh: Sheet, page_id: str, file_name: str):
    rows, _ = sh.as_dicts(POSTQUEUE_TAB)
    scoped, general = None, None
    for r in rows:
        if r.get("FileName", "").strip() != file_name:
            continue
        if r.get("Status", "").strip().lower() == "posted":
            continue
        row_page = r.get("PageId", "").strip()
        caption = r.get("Caption", "").strip()
        hashtags = r.get("Hashtags", "").strip()
        full_caption = f"{caption}\n{hashtags}" if hashtags else caption
        if row_page == page_id:
            scoped = (full_caption, r["_row"])
        elif not row_page and general is None:
            general = (full_caption, r["_row"])
    return scoped or general


def postqueue_mark_posted(sh: Sheet, row_num: int):
    rows, col_index = sh.as_dicts(POSTQUEUE_TAB)
    status_idx = col_index.get("status")
    if status_idx is not None:
        sh.write_cell(POSTQUEUE_TAB, row_num, status_idx, "Posted")


# ─────────────────────────────────────────────────────────────────────────────
# FACEBOOK STORAGE STATE resolution (per page)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_storage_state(page_id: str, sheet_json: str | None) -> str | None:
    if sheet_json:
        try:
            json.loads(sheet_json)
            return sheet_json
        except json.JSONDecodeError:
            fail(page_id, "FbStorageState in sheet is not valid JSON")

    env_val = os.environ.get(FB_STORAGE_STATE_ENV)
    if env_val:
        warn(page_id, "Falling back to shared FB_STORAGE_STATE env var (seed only — "
                       "add this page's own session to its FbStorageState cell)")
        return env_val

    local = STORAGE_STATE_DIR / f"{page_id}.json"
    if local.exists():
        info(page_id, f"Using local cached session: {local}")
        return local.read_text(encoding="utf-8")

    fail(page_id, "No Facebook session available for this page")
    return None


def save_storage_state_everywhere(sh: Sheet, page_cfg: PageConfig, fresh_json: str):
    STORAGE_STATE_DIR.mkdir(exist_ok=True)
    (STORAGE_STATE_DIR / f"{page_cfg.page_id}.json").write_text(fresh_json, encoding="utf-8")

    col_index = get_pages_col_index(sh)
    col = col_index.get("fbstoragestate")
    if col is None:
        warn(page_cfg.page_id, "No FbStorageState column found — cannot persist session to sheet")
        return
    if len(fresh_json) > SHEETS_CELL_CHAR_LIMIT:
        warn(page_cfg.page_id, f"Session JSON is {len(fresh_json)} chars — over the "
                                f"{SHEETS_CELL_CHAR_LIMIT} cell limit, not saved to sheet this cycle")
        return
    sh.clear_cell(PAGES_TAB, page_cfg.row_num, col)
    sh.write_cell(PAGES_TAB, page_cfg.row_num, col, fresh_json)
    ok(page_cfg.page_id, "Refreshed FbStorageState in sheet")


# ─────────────────────────────────────────────────────────────────────────────
# PLAYWRIGHT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def is_picker_url(url): return any(x in url for x in ["device-based", "/caa/", "login/caa", "login/identifier"])
def is_hard_login_url(url): return "/login" in url and not is_picker_url(url)

def classify_url(url: str) -> str:
    if "checkpoint" in url: return "CHECKPOINT"
    if is_hard_login_url(url): return "LOGIN_WALL"
    if is_picker_url(url): return "DEVICE_PICKER"
    if "reels/create" in url: return "REELS_CREATE"
    if "facebook.com" in url: return "FACEBOOK_PAGE"
    return "OTHER"


FEED_SELECTORS = [
    '[aria-label="Home"]', '[data-pagelet="LeftRail"]', 'div[role="feed"]',
    '[aria-label="Create"]', 'span:has-text("What\'s on your mind?")',
    'div[aria-label="Stories"]', 'div[aria-label="Reels"]',
    'div[data-pagelet="FeedUnit_0"]', 'div[role="main"]',
]


async def save_screenshot(page_id, page, name: str):
    SCREENSHOTS_DIR.mkdir(exist_ok=True)
    try:
        await page.screenshot(path=str(SCREENSHOTS_DIR / f"{page_id}_{name}.png"), full_page=False)
    except Exception as e:
        warn(page_id, f"Screenshot failed: {e}")


# ── Human-like pacing helpers ───────────────────────────────────────────────
# These exist purely so the automation doesn't click/type/upload at
# machine speed with zero variance. Ranges come from the sheet's
# Settings.MinDelaySeconds / MaxDelaySeconds (defaults 2.5–7s), scaled by a
# per-call `weight` so short UI waits and long "reading the page" waits
# both feel natural without needing separate config knobs.

async def human_delay(cfg: "PageConfig", page_id: str = None, weight: float = 1.0, label: str = None):
    lo = max(0.2, cfg.delay_min_seconds * weight)
    hi = max(lo + 0.4, cfg.delay_max_seconds * weight)
    d = random.uniform(lo, hi)
    if label:
        info(page_id, f"⏳ pausing ~{d:.1f}s ({label})")
    await asyncio.sleep(d)
    return d


async def human_mouse_move(page_id, page, moves: int = None):
    """Glides the mouse through a few random intermediate points instead of
    teleporting the cursor, and pauses briefly between glides."""
    try:
        vp = page.viewport_size or {"width": 1280, "height": 900}
        moves = moves or random.randint(2, 5)
        for _ in range(moves):
            x = random.randint(40, max(41, vp["width"] - 40))
            y = random.randint(40, max(41, vp["height"] - 40))
            await page.mouse.move(x, y, steps=random.randint(12, 30))
            await asyncio.sleep(random.uniform(0.15, 0.5))
    except Exception:
        pass  # purely cosmetic — never let this break the actual flow


async def human_scroll(page_id, page, passes: int = None):
    """Scrolls the page in a few small, slightly uneven steps (with an
    occasional tiny scroll-back) rather than one abrupt jump."""
    try:
        passes = passes or random.randint(2, 4)
        for _ in range(passes):
            if random.random() < 0.15:
                dy = -random.randint(80, 220)   # small human "oops, scroll back up"
            else:
                dy = random.randint(150, 480)
            await page.mouse.wheel(0, dy)
            await asyncio.sleep(random.uniform(0.25, 0.85))
    except Exception:
        pass


async def nuke_continue_button(page_id, page) -> bool:
    SELECTORS = [
        '[aria-label^="Continue"]', '[aria-label*="Continue"]',
        'div[role="button"][aria-label^="Continue"]',
        'div[role="button"]:has-text("Continue")',
        'span:text-is("Continue")', 'span:has-text("Continue")', 'button:has-text("Continue")',
    ]
    url_before = page.url
    found_sel = None
    for _ in range(10):
        for sel in SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    found_sel = sel; break
            except Exception:
                pass
        if found_sel: break
        await asyncio.sleep(1)

    if not found_sel:
        try:
            hit = await page.evaluate("""() => {
                const c = Array.from(document.querySelectorAll('div[role="button"],a[role="button"],button,a,span[tabindex]'));
                const b = c.find(el => /^continue/i.test((el.textContent||el.innerText||el.getAttribute('aria-label')||'').trim()));
                if (!b) return null; b.click(); return true; }""")
            if hit:
                await asyncio.sleep(5)
                return page.url != url_before
        except Exception:
            pass
        try:
            await page.goto("https://www.facebook.com/?sk=h_chr", wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(5)
            return not is_picker_url(page.url) and not is_hard_login_url(page.url)
        except Exception:
            return False

    loc = page.locator(found_sel).first
    for method in [lambda: loc.click(timeout=5_000), lambda: loc.click(force=True, timeout=5_000),
                   lambda: loc.evaluate("el => el.click()")]:
        try:
            await method()
            await asyncio.sleep(5)
            if page.url != url_before:
                return True
        except Exception:
            pass
    return False


async def ensure_logged_in(page_id, page) -> bool:
    for attempt in range(6):
        url_type = classify_url(page.url)
        if url_type == "CHECKPOINT":
            fail(page_id, "Account checkpoint/restriction — manual action required")
            await save_screenshot(page_id, page, f"checkpoint_{attempt+1}")
            return False
        if url_type == "LOGIN_WALL":
            fail(page_id, "Hard login wall — session cookies EXPIRED")
            await save_screenshot(page_id, page, f"login_wall_{attempt+1}")
            return False
        if url_type == "DEVICE_PICKER":
            await nuke_continue_button(page_id, page)
            continue
        for sel in FEED_SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        await asyncio.sleep(4)
    fail(page_id, "Login check exhausted all attempts")
    await save_screenshot(page_id, page, "login_failed_final")
    return False


def _nonempty_lines(text: str) -> list[str]:
    return [l for l in text.split("\n") if l.strip()]


def _caption_preview(caption: str, limit: int = 160) -> str:
    flat = " ".join(caption.split())
    if len(flat) <= limit:
        return flat if flat else "(empty)"
    return flat[:limit].rstrip() + "…"


async def _clear_field(page, field):
    await field.click(timeout=5_000)
    await asyncio.sleep(0.3)
    await page.keyboard.press("Control+a")
    await asyncio.sleep(0.2)
    await page.keyboard.press("Backspace")
    await asyncio.sleep(0.2)


async def enter_caption_lexical(page_id, page, caption: str) -> bool:
    LEXICAL_SELECTORS = [
        'div[data-lexical-editor="true"][contenteditable="true"]',
        'div[contenteditable="true"][aria-placeholder="Describe your reel..."]',
        'div[contenteditable="true"][role="textbox"]',
        'div[contenteditable="true"]',
    ]
    expected_lines = len(_nonempty_lines(caption))

    async def strat_keyboard(field):
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.keyboard.type(line, delay=12)
            if i < len(lines) - 1:
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    async def strat_clipboard(field):
        await _clear_field(page, field)
        await page.evaluate("(t) => navigator.clipboard.writeText(t).catch(()=>{})", caption)
        await asyncio.sleep(0.3)
        await page.keyboard.press("Control+v")
        await asyncio.sleep(0.8)

    async def strat_exec_command(field):
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.evaluate(
                    """(el, t) => { el.focus(); document.execCommand('insertText', false, t); }""",
                    [field, line])
            if i < len(lines) - 1:
                await page.evaluate(
                    """(el) => { el.focus(); document.execCommand('insertParagraph', false, null); }""", field)
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    async def strat_input_event(field):
        await _clear_field(page, field)
        lines = caption.split("\n")
        for i, line in enumerate(lines):
            if line:
                await page.evaluate(
                    """(el, t) => {
                        el.focus();
                        const sel=window.getSelection(); const r=document.createRange();
                        r.selectNodeContents(el); r.collapse(false); sel.removeAllRanges(); sel.addRange(r);
                        el.dispatchEvent(new InputEvent('beforeinput',{inputType:'insertText',data:t,bubbles:true,cancelable:true}));
                        el.dispatchEvent(new InputEvent('input',{inputType:'insertText',data:t,bubbles:true}));
                    }""", [field, line])
            if i < len(lines) - 1:
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)

    for i, strategy in enumerate([strat_keyboard, strat_clipboard, strat_exec_command, strat_input_event], 1):
        for sel in LEXICAL_SELECTORS:
            try:
                field = page.locator(sel).first
                if await field.count() == 0:
                    continue
                await strategy(field)
                txt = await field.evaluate("el => (el.innerText || el.textContent || '').trim()")
                actual_lines = len(_nonempty_lines(txt))
                if txt and len(txt) > 2 and abs(actual_lines - expected_lines) <= 1:
                    ok(page_id, f"Caption entered via strategy {i} ({actual_lines}/{expected_lines} lines)")
                    return True
            except Exception as e:
                warn(page_id, f"Caption strategy {i}/{sel} raised: {e}")
    return False


LINK_REJECTION_SELECTORS = [
    'span:has-text("couldn\'t be shared")',
    'span:has-text("goes against our Community Standards")',
    'div:has-text("goes against our Community Standards")',
]


async def detect_link_rejection(page_id, page) -> bool:
    for sel in LINK_REJECTION_SELECTORS:
        try:
            if await page.locator(sel).count() > 0:
                return True
        except Exception:
            pass
    return False


async def run_upload_flow(page_id, page, caption: str, video_path: str, cfg: "PageConfig") -> dict:
    published = False
    link_rejected = False
    file_label = os.path.basename(video_path)

    print(f"┌────────────────────────────────────────────────────────────")
    print(f"│ ▶️  UPLOAD CYCLE — page: {page_id}")
    print(f"│    file:    {file_label}")
    print(f"│    caption: {_caption_preview(caption)}")
    print(f"└────────────────────────────────────────────────────────────")

    step(page_id, "Loading Facebook homepage")
    try:
        await page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        fail(page_id, f"Page load failed: {e}")
        return {"published": False, "link_rejected": False}
    await human_delay(cfg, page_id, weight=1.6, label="letting the homepage settle")
    await human_mouse_move(page_id, page)
    await human_scroll(page_id, page)

    if not await ensure_logged_in(page_id, page):
        return {"published": False, "link_rejected": False}
    ok(page_id, "Login confirmed")
    await human_delay(cfg, page_id, weight=0.6)

    step(page_id, "Navigating to Reels create")
    try:
        await page.goto("https://www.facebook.com/reels/create/", wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        fail(page_id, f"Nav to reels/create failed: {e}")
        return {"published": False, "link_rejected": False}
    await human_delay(cfg, page_id, weight=1.4, label="letting the Reels composer load")
    await human_mouse_move(page_id, page)

    step(page_id, "Attaching video")
    uploaded = False
    for sel in ['input[type="file"][accept*="video"]', 'input[type="file"]']:
        try:
            inp = page.locator(sel)
            if await inp.count() > 0:
                await inp.first.set_input_files(video_path)
                uploaded = True
                break
        except Exception as e:
            warn(page_id, f"Direct input {sel} failed: {e}")

    if not uploaded:
        for btn_name, sel in [
            ('Select video', 'div[role="button"]:has-text("Select video")'),
            ('Upload', 'div[role="button"]:has-text("Upload")'),
            ('Add video', 'div[role="button"]:has-text("Add video")'),
            ('aria-label', '[aria-label="Select video"]'),
        ]:
            el = page.locator(sel).first
            try:
                if await el.count() == 0:
                    continue
                async with page.expect_file_chooser(timeout=10_000) as fc_info:
                    await el.click(force=True)
                fc = await fc_info.value
                await fc.set_files(video_path)
                uploaded = True
                break
            except Exception as e:
                warn(page_id, f"Upload button '{btn_name}' failed: {e}")

    if not uploaded:
        fail(page_id, "Could not attach video")
        await save_screenshot(page_id, page, "no_upload")
        return {"published": False, "link_rejected": False}
    ok(page_id, "Video attached")
    await human_delay(cfg, page_id, weight=1.8, label="letting the video finish processing")
    await human_scroll(page_id, page)

    step(page_id, "Waiting for Next to become active")
    next_selectors = ['div[aria-label="Next"][role="button"]', 'div[role="button"]:has-text("Next")',
                       'span:has-text("Next")', 'button:has-text("Next")']
    next_ready = False
    for elapsed in range(0, 180, 5):
        for sel in next_selectors:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.get_attribute("aria-disabled") != "true":
                    next_ready = True; break
            except Exception:
                pass
        if next_ready: break
        await asyncio.sleep(5)
    if not next_ready:
        warn(page_id, "Next never became active after 3 min")

    CAPTION_SELECTORS = [
        'div[data-lexical-editor="true"][contenteditable="true"]',
        'div[contenteditable="true"][aria-placeholder="Describe your reel..."]',
        'div[contenteditable="true"][role="textbox"]', 'div[contenteditable="true"]',
    ]

    async def caption_visible():
        for sel in CAPTION_SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        return False

    async def click_next():
        for sel in next_selectors:
            try:
                btn = page.locator(sel).first
                if await btn.count() == 0 or await btn.get_attribute("aria-disabled") == "true":
                    continue
                await btn.scroll_into_view_if_needed(timeout=5_000)
                await btn.click(timeout=10_000)
                return True
            except Exception:
                pass
        return False

    caption_found = await caption_visible()
    for attempt in range(1, 4):
        if caption_found:
            break
        if not await click_next():
            break
        for _ in range(8):
            if await caption_visible():
                caption_found = True; break
            await asyncio.sleep(2)

    await human_mouse_move(page_id, page)
    await human_delay(cfg, page_id, weight=0.9, label="pausing before typing the caption")
    step(page_id, f"Entering caption: {_caption_preview(caption)}")
    caption_ok = await enter_caption_lexical(page_id, page, caption)
    if caption_ok:
        ok(page_id, f"Caption confirmed on page: {_caption_preview(caption)}")
    else:
        warn(page_id, "Caption entry unverified — continuing anyway")
    await save_screenshot(page_id, page, "after_caption")
    await human_delay(cfg, page_id, weight=0.7, label="re-reading the caption before posting")

    step(page_id, "Advancing to Post panel")

    async def post_visible():
        for sel in ['div[aria-label="Post"][role="button"]', 'div[role="button"]:text-is("Post")', 'span:text-is("Post")']:
            try:
                if await page.locator(sel).count() > 0:
                    return True
            except Exception:
                pass
        return False

    if not await post_visible():
        for sel in ['div[aria-label="Post"][role="button"]', 'div[role="button"]:text-is("Post")',
                    'div[aria-label="Next"][role="button"]', 'div[role="button"]:has-text("Next")',
                    'span:text-is("Post")', 'span:has-text("Next")']:
            try:
                btn = page.locator(sel).last
                if await btn.count() == 0 or await btn.get_attribute("aria-disabled") == "true":
                    continue
                await btn.scroll_into_view_if_needed(timeout=5_000)
                await btn.click(force=True)
                await asyncio.sleep(4)
                break
            except Exception:
                pass

    await human_mouse_move(page_id, page)
    await human_delay(cfg, page_id, weight=1.1, label="one last pause before hitting Post")
    step(page_id, "Clicking Post / Publish")
    post_selectors = [
        'div[aria-label="Post"][role="button"]', 'div[role="button"]:text-is("Post")',
        'span:text-is("Post")', 'div[aria-label="Publish"][role="button"]',
        'div[aria-label="Share now"][role="button"]', 'div[role="button"]:has-text("Post")',
        'div[role="button"]:has-text("Publish")', 'button[type="submit"]',
    ]
    post_clicked = False
    for sel in post_selectors:
        try:
            btn = page.locator(sel).last
            if await btn.count() == 0 or await btn.get_attribute("aria-disabled") == "true":
                continue
            await btn.scroll_into_view_if_needed(timeout=5_000)
            await btn.click(force=True)
            post_clicked = True
            await asyncio.sleep(5)
            break
        except Exception as e:
            warn(page_id, f"Post click '{sel}' failed: {e}")

    if not post_clicked:
        fail(page_id, "Could not click Post/Publish")
        await save_screenshot(page_id, page, "no_post_button")
        return {"published": False, "link_rejected": False}

    if await detect_link_rejection(page_id, page):
        link_rejected = True
        fail(page_id, "Facebook rejected the post: link violates Community Standards")
        await save_screenshot(page_id, page, "link_rejected")
        return {"published": False, "link_rejected": True}

    step(page_id, "Waiting for publish confirmation")
    confirm_selectors = ['span:has-text("Your reel is now shared")', 'span:has-text("Reel posted")',
                          'span:has-text("Published")', 'span:has-text("Your reel")', 'span:has-text("shared")']
    for elapsed in range(0, 60, 5):
        if await detect_link_rejection(page_id, page):
            link_rejected = True
            break
        for sel in confirm_selectors:
            try:
                if await page.locator(sel).count() > 0:
                    published = True; break
            except Exception:
                pass
        if published: break
        await asyncio.sleep(5)

    if link_rejected:
        fail(page_id, "Facebook rejected the post: link violates Community Standards")
        await save_screenshot(page_id, page, "link_rejected")
        return {"published": False, "link_rejected": True}

    if not published:
        try:
            gone = await page.locator('div[aria-label="Post"][role="button"]').count() == 0
            if gone and post_clicked and not await detect_link_rejection(page_id, page):
                published = True
        except Exception:
            pass

    await save_screenshot(page_id, page, "final_result")
    result_line = "✅ PUBLISHED" if published else ("🚫 REJECTED (link)" if link_rejected else "❌ NOT CONFIRMED")
    print(f"┌────────────────────────────────────────────────────────────")
    print(f"│ 🏁 RESULT — page: {page_id}")
    print(f"│    file:      {file_label}")
    print(f"│    caption:   {_caption_preview(caption)}")
    print(f"│    published: {result_line}")
    print(f"└────────────────────────────────────────────────────────────")
    if published:
        ok(page_id, "🎉 Published")
    else:
        warn(page_id, "Could not confirm publish — check screenshot")
    return {"published": published, "link_rejected": link_rejected}


# ─────────────────────────────────────────────────────────────────────────────
# PAGE WORKER
# ─────────────────────────────────────────────────────────────────────────────

class PageWorker:
    """One Facebook Page, handled by exactly one matrix job (its JOB_INDEX
    position in the Active pages list). Keeps a single browser context OPEN
    for the whole run and periodically refreshes FbStorageState back to the
    sheet so the login session never goes stale.

    Does NOT scrape/sync the page's display name or id from Facebook — this
    worker's only job is posting reels. Before every single post cycle it
    re-reads Settings + this page's row fresh from Google Sheets (see
    _refresh_config), so a caption/folder/link%/delay edit you make in the
    sheet while the workflow is already running is picked up on the very
    next cycle — no restart needed.

    No assignment/lock columns, no semaphore / thread-count gating —
    concurrency across pages is achieved purely by running separate GitHub
    Actions matrix jobs (separate processes/runners), one page per job."""

    def __init__(self, sh: Sheet, cfg: PageConfig, col_index: dict, once: bool):
        self.sh = sh
        self.cfg = cfg
        self.col_index = col_index
        self.once = once
        self.browser = None
        self.context = None

    async def run(self):
        await self._run_locked()

    async def _run_locked(self):
        cfg = self.cfg
        start = time.monotonic()
        async with async_playwright() as p:
            try:
                self.browser = await p.chromium.launch(
                    headless=True, timeout=30_000,
                    args=["--no-sandbox", "--disable-setuid-sandbox",
                          "--disable-blink-features=AutomationControlled",
                          "--disable-infobars", "--disable-dev-shm-usage",
                          "--single-process", "--no-zygote"],
                )
            except Exception as e:
                fail(cfg.page_id, f"Browser launch failed: {e}")
                return

            storage_state_json = resolve_storage_state(cfg.page_id, cfg.storage_state_json)
            if not storage_state_json:
                await self.browser.close()
                return

            try:
                state = json.loads(storage_state_json)
                self.context = await self.browser.new_context(
                    storage_state=state,
                    user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
                    viewport={"width": 1280, "height": 900}, locale="en-US",
                    timezone_id="Asia/Karachi", accept_downloads=True,
                )
                await self.context.grant_permissions(
                    ["clipboard-read", "clipboard-write"], origin="https://www.facebook.com")
            except Exception as e:
                fail(cfg.page_id, f"Context creation failed: {e}")
                await self.browser.close()
                return

            last_heartbeat = time.monotonic()
            try:
                while True:
                    status = await self._refresh_config()
                    cfg = self.cfg   # local alias may now point to a fresh object — re-bind

                    if status == "paused":
                        info(cfg.page_id, "Page is Paused in the sheet — skipping this cycle's post")
                    elif status == "missing":
                        warn(cfg.page_id, "This page's row is no longer in the sheet — skipping this cycle's post")
                    else:
                        await self._post_once()

                    await self._heartbeat()
                    last_heartbeat = time.monotonic()

                    if self.once:
                        break
                    elapsed_min = (time.monotonic() - start) / 60
                    if elapsed_min >= cfg.max_runtime_minutes:
                        info(cfg.page_id, f"Runtime window ({cfg.max_runtime_minutes}m) reached — stopping")
                        break

                    next_interval_min = random.randint(
                        cfg.loop_interval_min_minutes, cfg.loop_interval_max_minutes
                    ) if cfg.loop_interval_max_minutes > cfg.loop_interval_min_minutes else cfg.loop_interval_min_minutes
                    info(cfg.page_id, f"⏲️  Next post in ~{next_interval_min}m "
                                       f"(random range {cfg.loop_interval_min_minutes}-{cfg.loop_interval_max_minutes}m)")
                    remaining = next_interval_min * 60
                    while remaining > 0:
                        chunk = min(remaining, cfg.heartbeat_minutes * 60)
                        await asyncio.sleep(chunk)
                        remaining -= chunk
                        if (time.monotonic() - last_heartbeat) / 60 >= cfg.heartbeat_minutes:
                            await self._heartbeat()
                            last_heartbeat = time.monotonic()
                        if (time.monotonic() - start) / 60 >= cfg.max_runtime_minutes:
                            remaining = 0
            finally:
                await self.browser.close()
                ok(cfg.page_id, "Browser closed — cycle finished")

    async def _refresh_config(self) -> str:
        """Re-reads 'Settings' + this page's row in 'Pages' fresh from
        Google Sheets and rebuilds self.cfg from scratch. Called at the
        start of EVERY post cycle (not just at startup) so that captions,
        folders, link %, URL-replace settings, delay ranges, etc. edited in
        the sheet mid-run are picked up on the very next cycle — never the
        stale, first-loaded values.
        Returns "ok", "paused", or "missing"."""
        pid = self.cfg.page_id
        try:
            master = load_master_settings(self.sh)
            rows, _ = self.sh.as_dicts(PAGES_TAB)
            row = next((r for r in rows if r.get("PageId", "").strip() == pid), None)
            if row is None:
                return "missing"
            if (row.get("Status", "") or "Active").strip().lower() == "paused":
                return "paused"
            self.col_index = get_pages_col_index(self.sh)
            self.cfg = build_page_config(row, master, self.cfg.max_runtime_minutes)
            info(pid, "🔄 Reloaded latest Settings + Pages row from Google Sheet for this cycle")
            return "ok"
        except Exception as e:
            warn(pid, f"Could not refresh config from sheet this cycle (using last-known values): {e}")
            return "ok"

    async def _heartbeat(self):
        cfg = self.cfg
        try:
            fresh = await self.context.storage_state()
            fresh_json = json.dumps(fresh)
            save_storage_state_everywhere(self.sh, cfg, fresh_json)
        except Exception as e:
            warn(cfg.page_id, f"Heartbeat storage_state save failed: {e}")

    async def _post_once(self):
        cfg = self.cfg
        pid = cfg.page_id
        print(f"\n══════════════════════ [{pid}] NEW POST CYCLE — {now_iso()} ══════════════════════")
        step(pid, f"Post cycle starting (mode={cfg.post_mode})")
        info(pid, f"Resolved settings: UrlReplaceEnabled={cfg.url_replace_enabled}, "
                  f"UrlSwapOnRejectOnly={cfg.url_swap_on_reject_only}, "
                  f"UrlReplaceMode={cfg.url_replace_mode}, UrlReplaceCount={cfg.url_replace_count}, "
                  f"Link_Percentage={cfg.link_percentage}, LinkRejectMaxRetries={cfg.link_reject_max_retries}")

        claim_folder = mega_claim_folder(cfg.mega_folder, pid)

        try:
            leftover = mega_list_videos(pid, claim_folder, missing_dir_ok=True)
        except Exception as e:
            warn(pid, f"Could not check claim folder '{claim_folder}': {e}")
            leftover = []

        claimed_file, claimed_match = None, None

        if leftover:
            claimed_file = leftover[0]["Name"]
            info(pid, f"Resuming previously-claimed video: {claimed_file}")
            if cfg.post_mode == "queue":
                claimed_match = postqueue_find_for_file(self.sh, pid, claimed_file)
                if not claimed_match:
                    warn(pid, f"Resumed claim '{claimed_file}' has no PostQueue row anymore — returning to pool")
                    mega_return_to_pool(pid, claim_folder, cfg.mega_folder, claimed_file)
                    return
        else:
            try:
                videos = mega_list_videos(pid, cfg.mega_folder)
            except Exception as e:
                fail(pid, f"Could not list Mega folder: {e}")
                return
            if not videos:
                info(pid, "No videos pending — nothing to do this cycle")
                return

            for v in videos:
                name = v["Name"]
                match = None
                if cfg.post_mode == "queue":
                    match = postqueue_find_for_file(self.sh, pid, name)
                    if not match:
                        continue
                if mega_try_claim(pid, cfg.mega_folder, name, claim_folder):
                    claimed_file, claimed_match = name, match
                    ok(pid, f"Claimed video: {name}")
                    break
                info(pid, f"Lost the claim race on '{name}' to another worker — trying next")

            if not claimed_file:
                if cfg.post_mode == "queue":
                    warn(pid, "No pending video currently has a matching PostQueue caption — skipping this cycle")
                else:
                    warn(pid, "Could not claim any video this cycle (all lost the race) — skipping")
                return

        file_name = claimed_file

        pq_row_num = None
        base_caption, use_link = None, False

        if cfg.post_mode == "queue":
            caption, pq_row_num = claimed_match
            if not caption.strip():
                warn(pid, f"PostQueue row for '{file_name}' has an empty caption — returning video to pool")
                mega_return_to_pool(pid, claim_folder, cfg.mega_folder, file_name)
                return
        else:
            roll = random.randint(1, 100)
            use_link = roll <= cfg.link_percentage
            base_caption = cfg.caption if use_link else (cfg.without_link_caption or cfg.caption)
            if not base_caption.strip():
                fail(pid, "No caption configured (Caption / WithoutLinkCap both empty) — skipping")
                return

        def build_rotation_caption(exclude_rows: set[int], force_swap: bool):
            cap = base_caption
            rows_used, status_col = [], None
            should_swap = use_link and cfg.url_replace_enabled and (force_swap or not cfg.url_swap_on_reject_only)
            if should_swap:
                n_matches = len(URL_REGEX.findall(cap))
                n_to_replace = min(cfg.url_replace_count, n_matches) if n_matches else cfg.url_replace_count
                fetch_count = 1 if cfg.url_replace_mode == "same" else max(n_to_replace, 1)
                fetched_urls, fetched_rows, status_col = urls_get_next(
                    self.sh, pid, fetch_count + len(exclude_rows))
                pairs = [(u, r) for u, r in zip(fetched_urls, fetched_rows) if r not in exclude_rows][:fetch_count]
                new_urls = [u for u, _ in pairs]
                rows_used = [r for _, r in pairs]
                if new_urls:
                    n_eff = n_to_replace
                    if cfg.url_replace_mode == "unique" and len(new_urls) < n_eff:
                        n_eff = len(new_urls)
                    cap = replace_urls_in_caption(cap, new_urls, cfg.url_replace_mode, n_eff)
                else:
                    warn(pid, "No unused URLs available — posting caption without swap")
            return cap, rows_used, status_col

        used_url_rows, url_status_col = [], None
        if cfg.post_mode != "queue":
            caption, used_url_rows, url_status_col = build_rotation_caption(exclude_rows=set(), force_swap=False)

        with tempfile.TemporaryDirectory() as tmp:
            try:
                local_path = mega_download_video(pid, claim_folder, file_name, tmp)
            except Exception as e:
                fail(pid, f"Download failed: {e}")
                mega_return_to_pool(pid, claim_folder, cfg.mega_folder, file_name)
                return

            published = False
            tried_url_rows: set[int] = set()
            max_attempts = 1 + (cfg.link_reject_max_retries if cfg.post_mode != "queue" else 0)

            for attempt in range(1, max_attempts + 1):
                try:
                    page = await self.context.new_page()
                    result = await run_upload_flow(pid, page, caption, local_path, cfg)
                    await page.close()
                except Exception as e:
                    fail(pid, f"Upload flow crashed: {e}")
                    import traceback; print(traceback.format_exc())
                    result = {"published": False, "link_rejected": False}

                published = result.get("published", False)
                if published:
                    break

                if not result.get("link_rejected"):
                    break

                if used_url_rows:
                    urls_mark_posted(self.sh, used_url_rows, url_status_col, status_value="Rejected")
                    warn(pid, f"Blacklisted rejected URL row(s) {used_url_rows} as 'Rejected'")
                    tried_url_rows.update(used_url_rows)

                if attempt >= max_attempts:
                    warn(pid, "Link rejection retry limit reached — giving up this cycle")
                    break
                if not (use_link and cfg.url_replace_enabled):
                    warn(pid, "Link rejected but URL replacement isn't enabled for this page — cannot retry")
                    break

                info(pid, f"Retrying with a fresh URL (attempt {attempt + 1}/{max_attempts})")
                caption, used_url_rows, url_status_col = build_rotation_caption(
                    exclude_rows=tried_url_rows, force_swap=True)
                if not used_url_rows and use_link and cfg.url_replace_enabled:
                    warn(pid, "No more unused URLs left to retry with — giving up this cycle")
                    break

        if published:
            try:
                mega_move_to_uploaded(pid, claim_folder, cfg.mega_move_folder, file_name)
            except Exception as e:
                warn(pid, f"Move to uploaded failed (video stays claimed under {claim_folder} for next cycle): {e}")

            if cfg.post_mode == "queue" and pq_row_num:
                postqueue_mark_posted(self.sh, pq_row_num)
            if used_url_rows:
                urls_mark_posted(self.sh, used_url_rows, url_status_col, status_value="Posted")

            lastfile_col = self.col_index.get("lastpostedfile")
            if lastfile_col is not None:
                self.sh.write_cell(PAGES_TAB, cfg.row_num, lastfile_col, file_name)
        else:
            warn(pid, "Upload not confirmed — returning video to the shared pool for retry")
            mega_return_to_pool(pid, claim_folder, cfg.mega_folder, file_name)


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def _require_spreadsheet_id() -> str:
    spreadsheet_id = os.environ.get(CAPTIONS_SHEET_ID_ENV, "").strip()
    if not spreadsheet_id:
        raise RuntimeError(
            f"{CAPTIONS_SHEET_ID_ENV} is not set. Store your Google Sheet ID as a GitHub "
            f"secret (e.g. CAPTIONS_SHEET_ID) and pass it through as this env var — there is "
            f"no hardcoded fallback any more, by design, so the sheet ID never has to be "
            f"typed into a workflow_dispatch input or appear in run history."
        )
    return spreadsheet_id


async def main_async(once: bool):
    creds = build_google_creds()
    service = build_sheets_service(creds)
    spreadsheet_id = _require_spreadsheet_id()

    # Step 0, every run: self-heal the sheet (create any missing tab/header/
    # default setting/column). This is what used to require a manual
    # --setup-sheet and caused "Unable to parse range" errors when a new
    # sheet ID was used without it.
    sh = setup_sheet(service, spreadsheet_id)

    master = load_master_settings(sh)
    max_runtime = _to_int(master.get("maxruntimeminutes"), 300)

    requeue_task = None
    if not once:
        requeue_task = asyncio.create_task(schedule_self_requeue(max_runtime))

    active_pages = get_active_pages(sh)
    page_row = pick_page_for_job(active_pages, JOB_INDEX)

    if page_row is None:
        info(None, f"No page at position {JOB_INDEX} — only {len(active_pages)} Active "
                    f"page(s) currently in the sheet, nothing for this job to do this cycle")
        if not once:
            trigger_self_requeue()
        if requeue_task:
            requeue_task.cancel()
        return

    col_index = get_pages_col_index(sh)
    cfg = build_page_config(page_row, master, max_runtime)

    info(None, f"Job {JOB_INDEX} of {len(active_pages)} active page(s) → "
                f"page '{cfg.page_id}' ('{cfg.page_name}', row {cfg.row_num})")

    worker = PageWorker(sh, cfg, col_index, once)
    await worker.run()

    # Worker wrapped up before the runtime-window timer fired (e.g. it hit
    # its own MaxRuntimeMinutes slightly early, or --once) — requeue right
    # now instead of waiting for the background timer.
    if not once:
        trigger_self_requeue()
    if requeue_task:
        requeue_task.cancel()


def main():
    if "--setup-sheet" in sys.argv:
        creds = build_google_creds()
        service = build_sheets_service(creds)
        spreadsheet_id = _require_spreadsheet_id()
        setup_sheet(service, spreadsheet_id)
        return

    if "--count-pages" in sys.argv:
        # Used by the workflow's "prepare" job to size its matrix: prints
        # how many Active pages currently exist in the sheet (and their
        # PageId|PageName for a readable log), then exits. No browser,
        # no posting — just a Sheets read.
        creds = build_google_creds()
        service = build_sheets_service(creds)
        spreadsheet_id = _require_spreadsheet_id()
        sh = setup_sheet(service, spreadsheet_id)
        active = get_active_pages(sh)
        print(f"ACTIVE_PAGE_COUNT={len(active)}")
        for i, r in enumerate(active):
            pid_ = r.get("PageId", "").strip()
            pname = r.get("PageName", "").strip() or pid_
            print(f"ACTIVE_PAGE_{i}={pid_}|{pname}")
        return

    once = "--once" in sys.argv or bool(os.environ.get("RUN_ONCE"))
    print(f"🚀 Job index {JOB_INDEX} starting — "
          f"{'single cycle (--once)' if once else 'continuous loop'}")
    asyncio.run(main_async(once))
    print("✅ Run complete")


if __name__ == "__main__":
    main()
