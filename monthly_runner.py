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

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
MAX_RETRIES = 3          # retries per URL before giving up
RETRY_DELAY = 3          # seconds between retries
BATCH_SIZE  = 50
START_COL   = "DH"        # first column to write scraped values

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

# ✅ FIX: read checkpoint correctly — this is the LAST successfully written row
# We'll resume FROM this row (not skip it)
last_i = 0
if os.path.exists(checkpoint_file):
    try:
        last_i = int(open(checkpoint_file).read().strip())
    except:
        last_i = 0

log(f"🔖 Resuming from row index {last_i}")


# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Chrome...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
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
            log("✅ Cookies applied")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:60]}")

    return driver


# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    """
    Returns:
      - list of values (possibly empty) on success
      - "RESTART" if browser crashed
    """
    try:
        driver.get(url)

        # ✅ FIX: Use a CSS class wait instead of a brittle full XPath.
        # Wait for ANY element with this class to appear — much more resilient.
        WebDriverWait(driver, 45).until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "div.valueValue-l31H9iuA"
            ))
        )

        # Small extra wait to let all values render
        time.sleep(1.5)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None').strip()
            for el in soup.find_all(
                "div",
                class_="valueValue-l31H9iuA apply-common-tooltip"
            )
        ]
        return values

    except TimeoutException:
        log("⏱️ Timeout waiting for values")
        return []
    except NoSuchElementException:
        log("❌ Element not found")
        return []
    except WebDriverException as e:
        log(f"🛑 Browser crash: {str(e)[:80]}")
        return "RESTART"


def scrape_with_retry(driver, url, name, max_retries=MAX_RETRIES):
    """
    Wraps scrape_tradingview with retry logic and browser restart on crash.
    Returns (driver, values_list_or_empty).
    """
    for attempt in range(1, max_retries + 1):
        result = scrape_tradingview(driver, url)

        if result == "RESTART":
            log(f"♻️ Restarting browser (attempt {attempt})")
            try:
                driver.quit()
            except:
                pass
            driver = create_driver()
            # retry immediately after restart
            result = scrape_tradingview(driver, url)
            if result == "RESTART":
                log("🛑 Browser restart failed twice, skipping row")
                return driver, []

        if isinstance(result, list) and len(result) > 0:
            return driver, result

        # Empty result — wait and retry
        if attempt < max_retries:
            log(f"⚠️ Empty result for {name}, retry {attempt}/{max_retries} in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        else:
            log(f"❌ All {max_retries} attempts failed for {name}")

    return driver, []


# ---------------- FLUSH BATCH TO SHEETS ---------------- #
def flush_batch(sheet, batch_list):
    """Write a batch to Google Sheets, with quota-retry."""
    if not batch_list:
        return
    for attempt in range(3):
        try:
            sheet.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} rows")
            return
        except Exception as e:
            log(f"⚠️ Sheets API error: {e}")
            if "429" in str(e) or "quota" in str(e).lower():
                wait = 60 * (attempt + 1)
                log(f"⏳ Quota hit — sleeping {wait}s...")
                time.sleep(wait)
            else:
                time.sleep(5)
    log("❌ Batch failed after 3 attempts — data may be lost for this batch")


# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(7)   # Column G — URLs
    name_list    = sheet_main.col_values(1)   # Column A — names (for logging)

    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Loaded {len(company_list)} rows | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume from {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)


# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []

# Track which rows we actually attempted vs succeeded, for a summary at the end
attempted = 0
succeeded = 0
skipped_empty_url = 0
skipped_no_data = 0

try:
    for i in range(len(company_list)):

        # ✅ FIX 1: Shard filtering — each worker handles its own slice
        # Shard 0 of 4: rows 0,4,8,...   Shard 1: rows 1,5,9,...  etc.
        if SHARD_STEP > 1 and (i % SHARD_STEP) != SHARD_INDEX:
            continue

        # ✅ FIX 2: Resume — skip rows already processed in a previous run
        if i < last_i:
            continue

        # Hard cap (adjust per your sheet size)
        if i >= 2600:
            break

        url  = company_list[i].strip() if i < len(company_list) and company_list[i] else ""
        name = name_list[i].strip()    if i < len(name_list)    and name_list[i]    else f"Row {i+1}"

        # Skip header row (row 0 in 0-indexed = row 1 in sheet)
        if i == 0:
            log(f"⏭️ Skipping header row 1")
            continue

        # Skip blank URLs
        if not url:
            log(f"⏭️ [{i+1}] {name} — empty URL")
            skipped_empty_url += 1
            # ✅ FIX 3: Still advance checkpoint so we don't re-visit this row
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i+1}] {name}")
        attempted += 1

        driver, values = scrape_with_retry(driver, url, name)

        if values:
            target_row = i + 1  # Sheets rows are 1-indexed
            batch_list.append({
                "range": f"{START_COL}{target_row}",
                "values": [values]
            })
            succeeded += 1
            log(f"📦 Buffered {len(values)} values | batch {len(batch_list)}/{BATCH_SIZE}")
        else:
            skipped_no_data += 1
            log(f"⚠️ [{i+1}] {name} — no data after retries")

        # Flush batch to Sheets
        if len(batch_list) >= BATCH_SIZE:
            flush_batch(sheet_data, batch_list)
            batch_list = []

        # Save checkpoint AFTER successful processing
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(0.5)  # polite delay

finally:
    # Flush any remaining rows
    flush_batch(sheet_data, batch_list)
    batch_list = []

    try:
        driver.quit()
    except:
        pass

    log("=" * 50)
    log(f"🏁 Done | Attempted: {attempted} | Succeeded: {succeeded} | "
        f"No-data: {skipped_no_data} | Empty-URL: {skipped_empty_url}")
    log("=" * 50)
