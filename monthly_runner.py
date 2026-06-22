import sys
import os
import time
import json
import random
from datetime import date

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, WebDriverException
)

from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
def log(msg):
    print(msg, flush=True)


# ─────────────────────────────────────────────
# CONFIG & SHARDING
# ─────────────────────────────────────────────
SHARD_INDEX     = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP      = int(os.getenv("SHARD_STEP",  "1"))
MAX_RETRIES     = 3
RETRY_BASE_DELAY= 4       # exponential backoff base (seconds)
BATCH_SIZE      = 50
START_COL       = "D"
TV_BASE_URL     = "https://in.tradingview.com"

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

# Resume from last checkpoint (this is the NEXT row to process)
last_i = 0
if os.path.exists(checkpoint_file):
    try:
        last_i = int(open(checkpoint_file).read().strip())
        log(f"🔖 Checkpoint found — resuming from row index {last_i}")
    except Exception:
        last_i = 0
        log("⚠️  Could not read checkpoint — starting from row 0")
else:
    log("🔖 No checkpoint — starting from row 0")


# ─────────────────────────────────────────────
# BROWSER FACTORY  (with proper session/cookie)
# ─────────────────────────────────────────────
def _build_options() -> Options:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-notifications")
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    return opts


def _apply_cookies(driver: webdriver.Chrome) -> bool:
    """
    Loads cookies.json and injects them into the browser.
    Must be called AFTER driver.get() has already loaded the target domain,
    so the browser has a cookie jar for that domain.

    Returns True if cookies were applied, False otherwise.
    """
    cookie_path = "cookies.json"
    if not os.path.exists(cookie_path):
        log("ℹ️  No cookies.json found — continuing without session cookies")
        return False

    try:
        with open(cookie_path, "r") as f:
            cookies = json.load(f)

        added = 0
        for c in cookies:
            # Keep only fields the WebDriver API accepts
            clean = {k: v for k, v in c.items()
                     if k in ("name", "value", "path", "secure", "expiry", "domain", "sameSite")}
            # Some cookie files store httpOnly / sameParty — skip those keys
            try:
                driver.add_cookie(clean)
                added += 1
            except Exception as ce:
                log(f"   ⚠️  Skipped cookie '{c.get('name','?')}': {str(ce)[:60]}")

        log(f"✅ Applied {added}/{len(cookies)} cookies")
        return added > 0
    except Exception as e:
        log(f"⚠️  Cookie load error: {str(e)[:80]}")
        return False


def _validate_session(driver: webdriver.Chrome) -> bool:
    """
    Quick heuristic: checks that the page doesn't show a 'Sign In' button
    that would indicate we are NOT logged in.
    Returns True  → logged-in (or no login required)
    Returns False → definitely not logged in
    """
    try:
        page = driver.page_source.lower()
        # TradingView shows these text strings when not logged in
        if 'sign in' in page and 'my account' not in page:
            return False
        return True
    except Exception:
        return False  # treat unknown state as not-logged-in


def create_driver() -> webdriver.Chrome:
    """
    Spins up a Chrome instance, loads the TradingView homepage,
    injects session cookies, refreshes, and validates the session.
    """
    log("🌐 Initialising Chrome...")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=_build_options()
    )
    driver.set_page_load_timeout(45)

    # ── Step 1: Navigate to base domain first (cookies are domain-bound) ──
    try:
        driver.get(TV_BASE_URL)
        # Wait for the page to be at least partially loaded before injecting cookies
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        time.sleep(2)  # small extra buffer for JS hydration
    except Exception as e:
        log(f"⚠️  Could not load base URL: {str(e)[:80]}")

    # ── Step 2: Inject cookies ──
    cookies_ok = _apply_cookies(driver)

    # ── Step 3: Refresh so the server recognises the session ──
    if cookies_ok:
        try:
            driver.refresh()
            WebDriverWait(driver, 20).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)
        except Exception:
            pass

        # ── Step 4: Validate session ──
        if _validate_session(driver):
            log("✅ Session validated — logged in")
        else:
            log("⚠️  Session validation failed — cookies may be stale; continuing anyway")

    return driver


# ─────────────────────────────────────────────
# SCRAPER
# ─────────────────────────────────────────────

# Multiple CSS selectors to try in priority order.
# TradingView sometimes renames classes; having fallbacks makes this resilient.
VALUE_SELECTORS = [
    "div.valueValue-l31H9iuA.apply-common-tooltip",  # most specific
    "div.valueValue-l31H9iuA",                         # class only
    "[class*='valueValue']",                            # partial class match
    "[class*='apply-common-tooltip'][class*='value']",  # combined partial
]

def _extract_values(page_source: str) -> list:
    """Parse the page and extract all financial values using multiple strategies."""
    soup = BeautifulSoup(page_source, "html.parser")

    for selector in VALUE_SELECTORS:
        elements = soup.select(selector)
        if elements:
            values = []
            for el in elements:
                text = (el.get_text()
                        .replace('−', '-')
                        .replace('∅', 'None')
                        .replace('\u2212', '-')   # Unicode minus sign
                        .replace('\xa0', ' ')     # non-breaking space
                        .strip())
                if text:
                    values.append(text)
            if values:
                log(f"   ✔ Found {len(values)} values via selector: {selector}")
                return values

    log("   ✘ No values found with any selector")
    return []


def scrape_tradingview(driver: webdriver.Chrome, url: str):
    """
    Returns:
        list  — extracted values (may be empty if page loaded but had no data)
        "RESTART" — browser crashed / unrecoverable WebDriver error
    """
    try:
        driver.get(url)

        # Wait for ANY matching value element to appear
        found = False
        for selector in VALUE_SELECTORS:
            try:
                WebDriverWait(driver, 40).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                found = True
                break
            except TimeoutException:
                continue

        if not found:
            log("⏱️ Timeout — no value elements appeared")
            return []

        # Let remaining values finish rendering
        time.sleep(1.5)

        return _extract_values(driver.page_source)

    except TimeoutException:
        log("⏱️ Page load timeout")
        return []
    except NoSuchElementException:
        log("❌ Element not found")
        return []
    except WebDriverException as e:
        msg = str(e)
        # Distinguish a crash from a mere navigation error
        if any(k in msg for k in ("chrome not reachable", "session deleted",
                                   "no such session", "disconnected")):
            log(f"🛑 Browser crash detected: {msg[:80]}")
            return "RESTART"
        log(f"⚠️  WebDriverException (non-crash): {msg[:80]}")
        return []
    except Exception as e:
        log(f"⚠️  Unexpected error: {str(e)[:80]}")
        return []


def scrape_with_retry(driver: webdriver.Chrome, url: str, name: str):
    """
    Retries scraping up to MAX_RETRIES times with exponential backoff.
    Restarts the browser on crash and re-applies the session.
    Returns (driver, values_list).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        result = scrape_tradingview(driver, url)

        # ── Browser crash → restart & re-apply session ──
        if result == "RESTART":
            log(f"♻️  Restarting browser (attempt {attempt}/{MAX_RETRIES})...")
            try:
                driver.quit()
            except Exception:
                pass
            driver = create_driver()

            # One immediate retry after restart
            result = scrape_tradingview(driver, url)
            if result == "RESTART":
                log("🛑 Second crash after restart — skipping row")
                return driver, []
            # Fall through: result is now a list (possibly empty)

        if isinstance(result, list) and result:
            return driver, result

        # Empty list → wait with exponential back-off and retry
        if attempt < MAX_RETRIES:
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 1)
            log(f"   ⚠️  Empty result for '{name}' — retry {attempt}/{MAX_RETRIES} in {delay:.1f}s")
            time.sleep(delay)

    log(f"   ❌ All {MAX_RETRIES} attempts failed for '{name}'")
    return driver, []


# ─────────────────────────────────────────────
# GOOGLE SHEETS BATCH WRITER
# ─────────────────────────────────────────────
def flush_batch(sheet: gspread.Worksheet, batch_list: list):
    """
    Writes batch_list to Google Sheets.
    batch_list items must be dicts: {"range": "D5", "values": [[v1, v2, ...]]}

    gspread's batch_update expects the list directly.
    Retries up to 3× with increasing back-off on quota errors.
    """
    if not batch_list:
        return

    for attempt in range(1, 4):
        try:
            sheet.batch_update(batch_list, value_input_option="USER_ENTERED")
            log(f"🚀 Saved {len(batch_list)} rows to sheet")
            return
        except gspread.exceptions.APIError as e:
            status = e.response.status_code if hasattr(e, 'response') else 0
            log(f"   ⚠️  Sheets API error (HTTP {status}): {str(e)[:80]}")
            if status == 429:
                wait = 60 * attempt
                log(f"   ⏳ Quota hit — sleeping {wait}s...")
                time.sleep(wait)
            else:
                time.sleep(5 * attempt)
        except Exception as e:
            log(f"   ⚠️  Unexpected Sheets error: {str(e)[:80]}")
            time.sleep(5 * attempt)

    log("❌ Batch failed after 3 attempts — data lost for this batch")


def save_checkpoint(i: int):
    """Save the index of the NEXT row to process."""
    try:
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
    except Exception as e:
        log(f"⚠️  Checkpoint write failed: {e}")


# ─────────────────────────────────────────────
# GOOGLE SHEETS SETUP
# ─────────────────────────────────────────────
log("📊 Connecting to Google Sheets...")
try:
    gc           = gspread.service_account("credentials.json")
    sheet_main   = gc.open("Stock List").worksheet("Sheet1")
    sheet_data   = gc.open("MV2 for SQL").worksheet("Sheet36")

    company_list = sheet_main.col_values(7)   # Column G — URLs
    name_list    = sheet_main.col_values(1)   # Column A — names

    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Loaded {len(company_list)} rows | "
        f"Shard {SHARD_INDEX}/{SHARD_STEP} | "
        f"Resume from row index {last_i}")
except Exception as e:
    log(f"❌ Setup error: {e}")
    sys.exit(1)


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────
driver     = create_driver()
batch_list = []

attempted         = 0
succeeded         = 0
skipped_empty_url = 0
skipped_no_data   = 0

try:
    total = len(company_list)

    for i in range(total):

        # ── Shard filtering ──
        if SHARD_STEP > 1 and (i % SHARD_STEP) != SHARD_INDEX:
            continue

        # ── Resume from checkpoint ──
        if i < last_i:
            continue

        # ── Hard cap ──
        if i >= 2600:
            log("🏁 Reached row cap (2600) — stopping")
            break

        # ── Skip header row (index 0 = Sheet row 1) ──
        if i == 0:
            log("⏭️  Skipping header row")
            save_checkpoint(i)
            continue

        url  = company_list[i].strip() if i < len(company_list) and company_list[i] else ""
        name = name_list[i].strip()    if i < len(name_list)    and name_list[i]    else f"Row {i+1}"

        # ── Skip blank URLs ──
        if not url:
            log(f"⏭️  [{i+1}] '{name}' — empty URL")
            skipped_empty_url += 1
            save_checkpoint(i)
            continue

        log(f"🔍 [{i+1}/{total}] {name}")
        attempted += 1

        driver, values = scrape_with_retry(driver, url, name)

        if values:
            sheet_row = i + 1          # Sheets is 1-indexed
            batch_list.append({
                "range":  f"{START_COL}{sheet_row}",
                "values": [values]
            })
            succeeded += 1
            log(f"   📦 Buffered {len(values)} values | batch size {len(batch_list)}/{BATCH_SIZE}")
        else:
            skipped_no_data += 1
            log(f"   ⚠️  [{i+1}] '{name}' — no data after all retries")

        # ── Flush when batch is full ──
        if len(batch_list) >= BATCH_SIZE:
            flush_batch(sheet_data, batch_list)
            batch_list = []

        # ── Checkpoint after every row ──
        save_checkpoint(i)

        # Polite delay (randomised to look more human)
        time.sleep(random.uniform(0.4, 0.9))

finally:
    # Flush any remaining rows
    flush_batch(sheet_data, batch_list)
    batch_list = []

    try:
        driver.quit()
    except Exception:
        pass

    log("=" * 55)
    log(f"🏁 DONE  |  Attempted: {attempted}  |  Succeeded: {succeeded}  |"
        f"  No-data: {skipped_no_data}  |  Empty-URL: {skipped_empty_url}")
    log("=" * 55)
