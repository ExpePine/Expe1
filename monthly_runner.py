#!/usr/bin/env python3
"""
TradingView Key-Stats Scraper -> Google Sheets
================================================

Reads URLs from "Stock List"!Sheet1 (col G), scrapes the key-stats values
panel on each TradingView page with headless Chrome, and writes the values
into "MV2 for SQL"!Sheet36 starting at column D.

WHAT CHANGED VS. THE ORIGINAL SCRIPT
-------------------------------------
1. BLOCK DETECTION: TradingView sits behind Cloudflare. Long headless runs
   eventually get served a "Just a moment..." / CAPTCHA page instead of the
   real page. That page never contains the stats panel, so the old code's
   plain `TimeoutException` retry loop just burned 3 retries doing nothing
   useful. We now explicitly detect challenge/block pages and react
   differently (immediate driver+cookie refresh, escalating cooldown).
2. RESILIENT SELECTOR: the old code hard-coded the full hashed CSS class
   ("valueValue-l31H9iuA"). TradingView rotates that hash on deploys, which
   silently breaks scraping for everyone. We match on the stable class
   *prefix* instead (`div[class*="valueValue-"]`), which survives hash
   rotation.
3. SESSION RECYCLING: a single Chrome session run for an hour straight is
   an easy bot fingerprint. The driver is now recycled every N rows
   (default 75) even when nothing has failed, with a fresh UA + cookies.
4. EXPONENTIAL BACKOFF + JITTER: retries no longer hammer the server on a
   flat 3s cadence.
5. CRASH-SAFE BUFFERING: the in-memory batch destined for Sheets is mirrored
   to a local JSON file on every append and cleared only after a confirmed
   flush. If the process dies mid-batch (OOM, kill -9, host restart), the
   next run recovers and flushes that data instead of losing it.
6. ATOMIC CHECKPOINTING: checkpoint writes use write-tmp-then-rename so a
   crash mid-write can't corrupt the checkpoint file.
7. GRACEFUL SHUTDOWN: SIGINT/SIGTERM trigger a clean stop — flush whatever
   is buffered, save checkpoint, quit the browser — instead of losing the
   in-flight batch.
8. DEBUG CAPTURE: on timeout/empty/block results, a screenshot + HTML
   snapshot is saved locally (capped, auto-rotated) so you can actually see
   *why* a row failed instead of guessing from log text.
9. PER-ROW EXCEPTION ISOLATION: one unexpected exception on a single row
   (bad URL, parsing edge-case, etc.) is logged and skipped instead of
   killing the whole shard.

ENVIRONMENT VARIABLES (all optional, defaults match the original script)
--------------------------------------------------------------------------
SHARD_INDEX                 default "0"
SHARD_STEP                  default "1"
CHECKPOINT_FILE             default "checkpoint_{SHARD_INDEX}.txt"
PENDING_BATCH_FILE          default "pending_batch_{SHARD_INDEX}.json"
MAX_RETRIES                 default "3"
RETRY_DELAY                 default "3"      (base seconds, exponential backoff applied)
BATCH_SIZE                  default "50"
START_COL                   default "D"
HARD_CAP_ROW                default "2600"
ROWS_PER_DRIVER_RECYCLE     default "75"
BLOCK_STREAK_COOLDOWN_AFTER default "3"       (consecutive blocked rows before a long cooldown)
BLOCK_COOLDOWN_SECONDS      default "180"
DEBUG_DIR                   default "debug_captures"
MAX_DEBUG_FILES             default "40"
SHEET_MAIN_NAME             default "Stock List"
SHEET_MAIN_WORKSHEET        default "Sheet1"
SHEET_DATA_NAME             default "MV2 for SQL"
SHEET_DATA_WORKSHEET        default "Sheet36"
CREDENTIALS_FILE            default "credentials.json"
COOKIES_FILE                default "cookies.json"

Dependencies: selenium, webdriver-manager, beautifulsoup4, gspread
"""

import sys
import os
import time
import json
import random
import signal
import threading
from datetime import date

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
    InvalidSessionIdException,
)

from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager


# =====================================================================
# CONFIG
# =====================================================================
def env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


SHARD_INDEX = env_int("SHARD_INDEX", 0)
SHARD_STEP = env_int("SHARD_STEP", 1)

MAX_RETRIES = env_int("MAX_RETRIES", 3)
RETRY_DELAY = env_int("RETRY_DELAY", 3)            # base seconds for backoff
BATCH_SIZE = env_int("BATCH_SIZE", 50)
START_COL = os.getenv("START_COL", "D")
HARD_CAP_ROW = env_int("HARD_CAP_ROW", 2600)

ROWS_PER_DRIVER_RECYCLE = env_int("ROWS_PER_DRIVER_RECYCLE", 75)
BLOCK_STREAK_COOLDOWN_AFTER = env_int("BLOCK_STREAK_COOLDOWN_AFTER", 3)
BLOCK_COOLDOWN_SECONDS = env_int("BLOCK_COOLDOWN_SECONDS", 180)

DEBUG_DIR = os.getenv("DEBUG_DIR", "debug_captures")
MAX_DEBUG_FILES = env_int("MAX_DEBUG_FILES", 40)

SHEET_MAIN_NAME = os.getenv("SHEET_MAIN_NAME", "Stock List")
SHEET_MAIN_WORKSHEET = os.getenv("SHEET_MAIN_WORKSHEET", "Sheet1")
SHEET_DATA_NAME = os.getenv("SHEET_DATA_NAME", "MV2 for SQL")
SHEET_DATA_WORKSHEET = os.getenv("SHEET_DATA_WORKSHEET", "Sheet36")

CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
COOKIES_FILE = os.getenv("COOKIES_FILE", "cookies.json")

CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
PENDING_BATCH_FILE = os.getenv("PENDING_BATCH_FILE", f"pending_batch_{SHARD_INDEX}.json")

VALUE_SELECTOR = 'div[class*="valueValue-"]'   # resilient to CSS-module hash rotation

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

BLOCK_MARKERS = [
    "just a moment",
    "checking your browser",
    "cf-browser-verification",
    "attention required",
    "access denied",
    "are you a human",
    "are you a robot",
    "captcha",
    "unusual traffic",
    "verify you are human",
]


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =====================================================================
# GRACEFUL SHUTDOWN
# =====================================================================
shutdown_requested = threading.Event()


def _handle_signal(signum, frame):
    log(f"🛑 Signal {signum} received — finishing current row, then shutting down cleanly")
    shutdown_requested.set()


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# =====================================================================
# ATOMIC FILE HELPERS
# =====================================================================
def atomic_write(path, content):
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_checkpoint(path):
    if not os.path.exists(path):
        return 0
    try:
        return int(open(path).read().strip())
    except Exception:
        log(f"⚠️ Checkpoint file {path} unreadable, starting from 0")
        return 0


def write_checkpoint(path, row_index):
    atomic_write(path, str(row_index))


def load_pending_batch(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except Exception as e:
        log(f"⚠️ Pending batch file unreadable ({e}), ignoring it")
    return []


def save_pending_batch(path, batch_list):
    atomic_write(path, json.dumps(batch_list))


def clear_pending_batch(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# =====================================================================
# BLOCK / CHALLENGE DETECTION
# =====================================================================
def is_blocked_page(driver):
    try:
        title = (driver.title or "").lower()
        src_snippet = driver.page_source[:4000].lower()
    except Exception:
        return False
    return any(m in title or m in src_snippet for m in BLOCK_MARKERS)


# =====================================================================
# DEBUG CAPTURE
# =====================================================================
def save_debug_snapshot(driver, label):
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        ts = int(time.time() * 1000)
        base = os.path.join(DEBUG_DIR, f"{label}_{ts}")
        try:
            driver.save_screenshot(f"{base}.png")
        except Exception:
            pass
        try:
            with open(f"{base}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
        except Exception:
            pass
        _rotate_debug_files()
    except Exception as e:
        log(f"⚠️ Debug capture failed: {str(e)[:80]}")


def _rotate_debug_files():
    try:
        files = sorted(
            (os.path.join(DEBUG_DIR, f) for f in os.listdir(DEBUG_DIR)),
            key=os.path.getmtime,
        )
        excess = len(files) - MAX_DEBUG_FILES
        for f in files[:max(excess, 0)]:
            os.remove(f)
    except Exception:
        pass


# =====================================================================
# BROWSER FACTORY
# =====================================================================
def create_driver():
    log("🌐 Initializing Chrome...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )
    driver.set_page_load_timeout(40)

    # Light stealth: hide the most obvious automation flag.
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
        )
    except Exception:
        pass

    if os.path.exists(COOKIES_FILE):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(3)
            with open(COOKIES_FILE) as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie(
                        {k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")}
                    )
                except Exception:
                    continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:60]}")

    return driver


def safe_quit(driver):
    try:
        driver.quit()
    except Exception:
        pass


# =====================================================================
# SCRAPER
# =====================================================================
def scrape_tradingview(driver, url, wait_seconds=45):
    """
    Returns (status, values):
      status in {"OK", "EMPTY", "TIMEOUT", "BLOCKED", "CRASH"}
    """
    try:
        driver.get(url)
        time.sleep(2)  # let Cloudflare's challenge (if any) render before we wait on the real selector
        if is_blocked_page(driver):
            return "BLOCKED", []

        WebDriverWait(driver, wait_seconds).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, VALUE_SELECTOR))
        )
        time.sleep(1.5)  # let remaining values populate

        if is_blocked_page(driver):
            return "BLOCKED", []

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace("−", "-").replace("∅", "None").strip()
            for el in soup.select(VALUE_SELECTOR)
        ]
        return ("OK", values) if values else ("EMPTY", [])

    except TimeoutException:
        if is_blocked_page(driver):
            return "BLOCKED", []
        return "TIMEOUT", []
    except NoSuchElementException:
        return "EMPTY", []
    except InvalidSessionIdException:
        return "CRASH", []
    except WebDriverException as e:
        log(f"🛑 Browser crash: {str(e)[:80]}")
        return "CRASH", []


class RunState:
    """Mutable counters shared across the scrape loop."""

    def __init__(self):
        self.block_streak = 0
        self.rows_since_recycle = 0


def scrape_with_retry(driver, url, name, state, max_retries=MAX_RETRIES):
    """
    Wraps scrape_tradingview with status-aware retry logic.
    Returns (driver, values_list_or_empty).
    """
    for attempt in range(1, max_retries + 1):
        if shutdown_requested.is_set():
            return driver, []

        status, values = scrape_tradingview(driver, url)

        if status == "OK":
            state.block_streak = 0
            return driver, values

        if status == "BLOCKED":
            state.block_streak += 1
            log(f"🚧 Block/challenge page detected for {name} (streak={state.block_streak})")
            save_debug_snapshot(driver, f"blocked_{name}")
            safe_quit(driver)
            driver = create_driver()
            state.rows_since_recycle = 0

            if state.block_streak >= BLOCK_STREAK_COOLDOWN_AFTER:
                log(f"🧊 {state.block_streak} consecutive blocks — cooling down {BLOCK_COOLDOWN_SECONDS}s")
                time.sleep(BLOCK_COOLDOWN_SECONDS)
                state.block_streak = 0
            continue  # retry this same attempt count after fresh driver

        if status == "CRASH":
            log(f"♻️ Restarting browser after crash (attempt {attempt})")
            safe_quit(driver)
            driver = create_driver()
            state.rows_since_recycle = 0
            continue

        # TIMEOUT or EMPTY
        save_debug_snapshot(driver, f"{status.lower()}_{name}")
        if attempt < max_retries:
            backoff = RETRY_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1.5)
            log(f"⚠️ {status.title()} for {name}, retry {attempt}/{max_retries} in {backoff:.1f}s...")
            time.sleep(backoff)
        else:
            log(f"❌ All {max_retries} attempts failed for {name}")

    return driver, []


# =====================================================================
# FLUSH BATCH TO SHEETS (with crash-safe local mirror)
# =====================================================================
def flush_batch(sheet, batch_list):
    """Write a batch to Google Sheets, with quota-retry. Returns True on success."""
    if not batch_list:
        return True
    for attempt in range(3):
        try:
            sheet.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} rows")
            return True
        except Exception as e:
            log(f"⚠️ Sheets API error: {e}")
            if "429" in str(e) or "quota" in str(e).lower():
                wait = 60 * (attempt + 1)
                log(f"⏳ Quota hit — sleeping {wait}s...")
                time.sleep(wait)
            else:
                time.sleep(5)
    log("❌ Batch failed after 3 attempts — data kept in local pending-batch file for next run")
    return False


# =====================================================================
# MAIN
# =====================================================================
def main():
    last_i = read_checkpoint(CHECKPOINT_FILE)
    log(f"🔖 Resuming from row index {last_i}")

    log("📊 Connecting to Google Sheets...")
    try:
        gc = gspread.service_account(CREDENTIALS_FILE)
        sheet_main = gc.open(SHEET_MAIN_NAME).worksheet(SHEET_MAIN_WORKSHEET)
        sheet_data = gc.open(SHEET_DATA_NAME).worksheet(SHEET_DATA_WORKSHEET)

        company_list = sheet_main.col_values(7)   # Column G — URLs
        name_list = sheet_main.col_values(1)      # Column A — names (for logging)

        log(f"✅ Loaded {len(company_list)} rows | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume from {last_i}")
    except Exception as e:
        log(f"❌ Setup Error: {e}")
        sys.exit(1)

    # Recover any batch left over from a previous run that died mid-flight.
    batch_list = load_pending_batch(PENDING_BATCH_FILE)
    if batch_list:
        log(f"♻️ Found {len(batch_list)} unflushed rows from a previous run — flushing now")
        if flush_batch(sheet_data, batch_list):
            clear_pending_batch(PENDING_BATCH_FILE)
            batch_list = []

    driver = create_driver()
    state = RunState()

    attempted = 0
    succeeded = 0
    skipped_empty_url = 0
    skipped_no_data = 0
    errors = 0

    try:
        for i in range(len(company_list)):
            if shutdown_requested.is_set():
                log("🛑 Shutdown requested — stopping loop")
                break

            if SHARD_STEP > 1 and (i % SHARD_STEP) != SHARD_INDEX:
                continue

            if i < last_i:
                continue

            if i >= HARD_CAP_ROW:
                break

            try:
                url = company_list[i].strip() if i < len(company_list) and company_list[i] else ""
                name = name_list[i].strip() if i < len(name_list) and name_list[i] else f"Row {i + 1}"

                if i == 0:
                    log("⏭️ Skipping header row 1")
                    write_checkpoint(CHECKPOINT_FILE, i + 1)
                    continue

                if not url:
                    log(f"⏭️ [{i + 1}] {name} — empty URL")
                    skipped_empty_url += 1
                    write_checkpoint(CHECKPOINT_FILE, i + 1)
                    continue

                # Recycle the browser periodically to avoid long-session
                # fingerprinting, independent of whether anything failed.
                if state.rows_since_recycle >= ROWS_PER_DRIVER_RECYCLE:
                    log(f"♻️ Recycling browser after {state.rows_since_recycle} rows")
                    safe_quit(driver)
                    driver = create_driver()
                    state.rows_since_recycle = 0

                log(f"🔍 [{i + 1}] {name}")
                attempted += 1

                driver, values = scrape_with_retry(driver, url, name, state)
                state.rows_since_recycle += 1

                if values:
                    target_row = i + 1
                    batch_list.append({"range": f"{START_COL}{target_row}", "values": [values]})
                    save_pending_batch(PENDING_BATCH_FILE, batch_list)
                    succeeded += 1
                    log(f"📦 Buffered {len(values)} values | batch {len(batch_list)}/{BATCH_SIZE}")
                else:
                    skipped_no_data += 1
                    log(f"⚠️ [{i + 1}] {name} — no data after retries")

                if len(batch_list) >= BATCH_SIZE:
                    if flush_batch(sheet_data, batch_list):
                        batch_list = []
                        clear_pending_batch(PENDING_BATCH_FILE)

                write_checkpoint(CHECKPOINT_FILE, i + 1)
                time.sleep(0.5 + random.uniform(0, 1.0))  # polite, jittered delay

            except Exception as row_err:
                errors += 1
                log(f"💥 Unexpected error on row {i + 1}: {str(row_err)[:200]}")
                # Don't advance checkpoint past a row we never confirmed —
                # next run will retry it.
                continue

    finally:
        flush_batch(sheet_data, batch_list)
        if not batch_list:
            clear_pending_batch(PENDING_BATCH_FILE)

        safe_quit(driver)

        summary = {
            "attempted": attempted,
            "succeeded": succeeded,
            "no_data": skipped_no_data,
            "empty_url": skipped_empty_url,
            "row_errors": errors,
            "date": date.today().isoformat(),
            "shard_index": SHARD_INDEX,
            "shard_step": SHARD_STEP,
        }
        try:
            atomic_write(f"summary_{SHARD_INDEX}.json", json.dumps(summary, indent=2))
        except Exception:
            pass

        log("=" * 50)
        log(
            f"🏁 Done | Attempted: {attempted} | Succeeded: {succeeded} | "
            f"No-data: {skipped_no_data} | Empty-URL: {skipped_empty_url} | "
            f"Row errors: {errors}"
        )
        log("=" * 50)


if __name__ == "__main__":
    main()
