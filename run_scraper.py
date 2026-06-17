import sys
import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

# Force immediate log output
def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX  = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP   = int(os.getenv("SHARD_STEP",  "1"))
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "3"))   # NEW: per-URL retry attempts
RETRY_DELAY  = int(os.getenv("RETRY_DELAY", "5"))   # NEW: seconds between retries

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 0

# Skipped-URL log so you can re-run failures later
skip_log_file = f"skipped_{SHARD_INDEX}.jsonl"

def log_skip(i, name, url, reason):
    entry = json.dumps({"row": i, "name": name, "url": url, "reason": reason})
    with open(skip_log_file, "a") as f:
        f.write(entry + "\n")
    log(f"⏭️  Skipped [{i}] {name} — {reason}")

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    # NEW: extra stability flags
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_experimental_option('excludeSwitches', ['enable-logging'])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    driver.set_page_load_timeout(40)

    # ---- COOKIE LOGIC (UNCHANGED) ----
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(3)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:60]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #

# NEW: multiple fallback CSS/XPath selectors so a TradingView layout change
# doesn't silently produce empty results.
VALUE_SELECTORS = [
    # Primary (original)
    ("css",   "div.valueValue-l31H9iuA.apply-common-tooltip"),
    # Fallback 1 — partial class match (survives hash changes)
    ("css",   "[class*='valueValue'][class*='apply-common-tooltip']"),
    # Fallback 2 — data attribute sometimes present
    ("css",   "[data-name='financial-value']"),
]

# NEW: anchor XPath used purely to confirm the page has loaded meaningful data
ANCHOR_XPATH = (
    '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]'
    '/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
)

def extract_values(driver):
    """Try each selector in order; return the first non-empty list."""
    soup = BeautifulSoup(driver.page_source, "html.parser")
    for kind, selector in VALUE_SELECTORS:
        if kind == "css":
            elements = soup.select(selector)
        else:
            # lxml XPath via selenium (not bs4) — kept for completeness
            elements = driver.find_elements(By.XPATH, selector)
        if elements:
            values = [
                el.get_text().replace('−', '-').replace('∅', 'None')
                for el in elements
            ]
            log(f"   ✔ Values found via [{selector}]: {len(values)} items")
            return values
    return []

def scrape_tradingview(driver, url):
    """
    Returns:
        list[str]   — scraped values (possibly empty)
        "RESTART"   — browser crashed, caller should restart
        "SKIP"      — page loaded but produced no data after retries
    """
    try:
        driver.get(url)
        # Wait for the anchor element (page content present)
        try:
            WebDriverWait(driver, 45).until(
                EC.visibility_of_element_located((By.XPATH, ANCHOR_XPATH))
            )
        except TimeoutException:
            # Anchor not found — page may have changed layout; still try selectors
            log("   ⚠️ Anchor timeout — attempting fallback extraction")

        values = extract_values(driver)
        return values if values else []

    except (NoSuchElementException,):
        return []
    except WebDriverException as e:
        log(f"🛑 WebDriverException: {str(e)[:80]}")
        return "RESTART"
    except Exception as e:
        log(f"⚠️ Unexpected scrape error: {str(e)[:80]}")
        return []

# NEW: retry wrapper — tries up to MAX_RETRIES times before giving up
def scrape_with_retry(driver_ref, url, name):
    """
    Returns (driver, values_or_none).
    `driver_ref` is a list so we can swap out the driver on RESTART.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        log(f"   Attempt {attempt}/{MAX_RETRIES}")
        result = scrape_tradingview(driver_ref[0], url)

        if result == "RESTART":
            log("🔄 Restarting browser...")
            try:
                driver_ref[0].quit()
            except:
                pass
            driver_ref[0] = create_driver()
            # After restart, wait a moment then retry
            time.sleep(RETRY_DELAY)
            continue

        if result:          # non-empty list — success
            return result

        # Empty result — wait then retry
        if attempt < MAX_RETRIES:
            log(f"   ⚠️ Empty result, retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    return None   # all attempts exhausted

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc           = gspread.service_account("credentials.json")
    sheet_main   = gc.open("Stock List").worksheet("Sheet1")
    sheet_data   = gc.open("Tradingview Data Reel Experimental May").worksheet("Sheet20")

    company_list = sheet_main.col_values(5)
    name_list    = sheet_main.col_values(1)

    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Setup complete | Shard {SHARD_INDEX} | Resume index {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver_ref = [create_driver()]   # list wrapper so retry wrapper can swap it
batch_list = []
BATCH_SIZE = 50

# Track per-run stats
stats = {"scraped": 0, "skipped": 0, "saved": 0}

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX:
            continue
        if i >= 2500:
            break

        url  = company_list[i]
        name = name_list[i] if i < len(name_list) else f"Row {i}"

        # NEW: skip obviously bad URLs immediately
        if not url or not url.strip().startswith("http"):
            log_skip(i, name, url, "invalid_url")
            stats["skipped"] += 1
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i}] Scraping: {name}  ({url[:60]})")

        values = scrape_with_retry(driver_ref, url, name)

        if values:
            target_row = i + 1
            batch_list.append({
                "range":  f"A{target_row}",
                "values": [[name, current_date] + values]
            })
            stats["scraped"] += 1
            log(f"📦 Buffered ({len(batch_list)}/{BATCH_SIZE}) | total scraped: {stats['scraped']}")
        else:
            log_skip(i, name, url, "no_data_after_retries")
            stats["skipped"] += 1

        # Flush batch to Sheets
        if len(batch_list) >= BATCH_SIZE:
            for flush_attempt in range(3):   # NEW: retry the write too
                try:
                    sheet_data.batch_update(batch_list)
                    stats["saved"] += len(batch_list)
                    log(f"🚀 Saved {len(batch_list)} rows | total saved: {stats['saved']}")
                    batch_list = []
                    break
                except Exception as e:
                    log(f"⚠️ API Error (attempt {flush_attempt+1}/3): {e}")
                    if "429" in str(e):
                        log("⏳ Quota hit, sleeping 60s...")
                        time.sleep(60)
                    else:
                        time.sleep(10)

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(0.5)

finally:
    # Flush remaining rows
    if batch_list:
        for flush_attempt in range(3):
            try:
                sheet_data.batch_update(batch_list)
                stats["saved"] += len(batch_list)
                log(f"✅ Final save: {len(batch_list)} rows")
                batch_list = []
                break
            except Exception as e:
                log(f"⚠️ Final flush error: {e}")
                time.sleep(10)

    try:
        driver_ref[0].quit()
    except:
        pass

    log(
        f"\n🏁 Scraping completed | "
        f"scraped={stats['scraped']} | "
        f"skipped={stats['skipped']} | "
        f"saved={stats['saved']}"
    )
    log(f"📋 Skipped URLs logged to: {skip_log_file}")
