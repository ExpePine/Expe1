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
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
    InvalidSessionIdException,
    SessionNotCreatedException,
    StaleElementReferenceException
)

import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))
MAX_RETRIES = 3
RETRY_DELAY = 3
BATCH_SIZE  = 50
START_COL   = "A"
MAX_ROWS    = 2600

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

last_i = 0
if os.path.exists(checkpoint_file):
    try:
        last_i = int(open(checkpoint_file).read().strip())
    except:
        last_i = 0

log(f"🔖 Resuming from row index {last_i}")

# Keep same path/selector focus
PRIMARY_SELECTOR = "div.valueValue-l31H9iuA.apply-common-tooltip"
FALLBACK_SELECTOR = "div.valueValue-l31H9iuA"

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
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-renderer-backgrounding")
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
    driver.set_page_load_timeout(45)
    driver.set_script_timeout(30)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(3)
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    cookie = {
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "domain", "secure", "expiry")
                    }
                    driver.add_cookie(cookie)
                except:
                    continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")

    return driver

def safe_quit(driver):
    try:
        if driver:
            driver.quit()
    except:
        pass

# ---------------- HELPER: EXTRACT TEXT FROM SAME PLACE ---------------- #
def extract_values_from_same_place(driver):
    values = []

    selectors = [PRIMARY_SELECTOR, FALLBACK_SELECTOR]

    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if not elements:
                continue

            temp = []
            for el in elements:
                try:
                    txt = (
                        el.get_attribute("innerText")
                        or el.get_attribute("textContent")
                        or el.text
                        or ""
                    )
                    txt = txt.replace('−', '-').replace('∅', 'None').strip()
                    if txt:
                        temp.append(txt)
                except StaleElementReferenceException:
                    continue
                except Exception:
                    continue

            if temp:
                values = temp
                break

        except Exception:
            continue

    return values

def wait_for_values(driver, timeout=45, poll=1.0):
    end_time = time.time() + timeout
    last_count = 0

    while time.time() < end_time:
        values = extract_values_from_same_place(driver)
        if values:
            return values

        try:
            count1 = len(driver.find_elements(By.CSS_SELECTOR, PRIMARY_SELECTOR))
            count2 = len(driver.find_elements(By.CSS_SELECTOR, FALLBACK_SELECTOR))
            last_count = max(count1, count2)
        except Exception:
            pass

        time.sleep(poll)

    log(f"⏱️ Timeout waiting for non-empty values | seen elements count={last_count}")
    return []

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    """
    Returns:
      - list of values on success
      - [] on timeout/failure
      - "RESTART" if browser crashed
    """
    try:
        driver.get(url)

        WebDriverWait(driver, 30).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

        time.sleep(2)

        values = wait_for_values(driver, timeout=30, poll=1.0)
        return values

    except TimeoutException:
        log("⏱️ Timeout waiting for page load")
        return []
    except NoSuchElementException:
        log("❌ Element not found")
        return []
    except (InvalidSessionIdException, SessionNotCreatedException):
        log("🛑 Invalid session/browser closed")
        return "RESTART"
    except WebDriverException as e:
        msg = str(e).lower()
        log(f"🛑 Browser/WebDriver issue: {str(e)[:120]}")
        if (
            "session deleted" in msg
            or "tab crashed" in msg
            or "invalid session id" in msg
            or "chrome not reachable" in msg
            or "session not created" in msg
        ):
            return "RESTART"
        return []
    except Exception as e:
        log(f"❌ Unexpected scrape error: {str(e)[:120]}")
        return []

def scrape_with_retry(driver, url, name, max_retries=MAX_RETRIES):
    """
    Wraps scrape_tradingview with retry logic and browser restart on crash.
    Returns (driver, values_list_or_empty).
    """
    for attempt in range(1, max_retries + 1):
        result = scrape_tradingview(driver, url)

        if result == "RESTART":
            log(f"♻️ Restarting browser for {name} (attempt {attempt})")
            safe_quit(driver)
            time.sleep(2)
            driver = create_driver()
            result = scrape_tradingview(driver, url)

            if result == "RESTART":
                log("🛑 Browser restart failed twice, skipping row")
                return driver, []

        if isinstance(result, list) and len(result) > 0:
            return driver, result

        if attempt < max_retries:
            log(f"⚠️ Empty result for {name}, retry {attempt}/{max_retries} in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        else:
            log(f"❌ All {max_retries} attempts failed for {name}")

    return driver, []

# ---------------- FLUSH BATCH TO SHEETS ---------------- #
def flush_batch(sheet, batch_list):
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

    log("❌ Batch failed after 3 attempts — data may be lost for this batch")
    return False

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet36")

    company_list = sheet_main.col_values(7)   # Column G — URLs
    name_list    = sheet_main.col_values(1)   # Column A — names

    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Loaded {len(company_list)} rows | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume from {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []

attempted = 0
succeeded = 0
skipped_empty_url = 0
skipped_no_data = 0
pending_checkpoint = last_i

try:
    for i in range(len(company_list)):

        if SHARD_STEP > 1 and (i % SHARD_STEP) != SHARD_INDEX:
            continue

        if i < last_i:
            continue

        if i >= MAX_ROWS:
            break

        url  = company_list[i].strip() if i < len(company_list) and company_list[i] else ""
        name = name_list[i].strip()    if i < len(name_list) and name_list[i] else f"Row {i+1}"

        if i == 0:
            log("⏭️ Skipping header row 1")
            continue

        if not url:
            log(f"⏭️ [{i+1}] {name} — empty URL")
            skipped_empty_url += 1
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i+1}] {name}")
        attempted += 1

        driver, values = scrape_with_retry(driver, url, name)

        if values:
            target_row = i + 1
            batch_list.append({
                "range": f"{START_COL}{target_row}",
                "values": [values]
            })
            succeeded += 1
            pending_checkpoint = i + 1
            log(f"✅ Got {len(values)} values | batch {len(batch_list)}/{BATCH_SIZE}")
        else:
            skipped_no_data += 1
            pending_checkpoint = i + 1
            log(f"⚠️ [{i+1}] {name} — no data after retries")

        if len(batch_list) >= BATCH_SIZE:
            ok = flush_batch(sheet_data, batch_list)
            if ok:
                batch_list = []
                with open(checkpoint_file, "w") as f:
                    f.write(str(pending_checkpoint))

        time.sleep(0.7)

finally:
    ok = flush_batch(sheet_data, batch_list)
    if ok:
        with open(checkpoint_file, "w") as f:
            f.write(str(pending_checkpoint))

    safe_quit(driver)

    log("=" * 50)
    log(f"🏁 Done | Attempted: {attempted} | Succeeded: {succeeded} | No-data: {skipped_no_data} | Empty-URL: {skipped_empty_url}")
    log("=" * 50)
