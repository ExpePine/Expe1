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
    SessionNotCreatedException
)

import gspread

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
MAX_RETRIES = 3
RETRY_DELAY = 5
BATCH_SIZE = 50
START_COL = "A"
MAX_ROWS = 2600

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

last_i = 0
if os.path.exists(checkpoint_file):
    try:
        last_i = int(open(checkpoint_file).read().strip())
    except:
        last_i = 0

log(f"🔖 Resuming from row index {last_i}")

# ---------------- DRIVER ---------------- #
def create_driver():
    log("🌐 Initializing Chrome...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--remote-debugging-port=9222")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_experimental_option("prefs", {
        "profile.managed_default_content_settings.images": 2
    })
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    # Use Selenium Manager / local matching driver
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(50)
    driver.set_script_timeout(30)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    cookie = {
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "domain", "secure", "expiry")
                    }
                    if "sameSite" in c:
                        cookie["sameSite"] = c["sameSite"]
                    driver.add_cookie(cookie)
                except Exception:
                    continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:100]}")

    return driver

def safe_quit(driver):
    try:
        if driver:
            driver.quit()
    except:
        pass

def restart_driver(driver):
    safe_quit(driver)
    time.sleep(2)
    return create_driver()

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url):
    try:
        driver.get(url)

        WebDriverWait(driver, 45).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.valueValue-l31H9iuA"))
        )

        time.sleep(2)

        elements = driver.find_elements(By.CSS_SELECTOR, "div.valueValue-l31H9iuA")
        values = []
        for el in elements:
            try:
                text = el.text.replace('−', '-').replace('∅', 'None').strip()
                if text:
                    values.append(text)
            except:
                continue

        return values

    except TimeoutException:
        log("⏱️ Timeout waiting for values")
        return []
    except (InvalidSessionIdException, SessionNotCreatedException):
        log("🛑 Invalid or closed session")
        return "RESTART"
    except WebDriverException as e:
        msg = str(e).lower()
        log(f"🛑 WebDriverException: {str(e)[:200]}")
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
        log(f"❌ Unexpected scrape error: {str(e)[:200]}")
        return []

def scrape_with_retry(driver, url, name, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        result = scrape_tradingview(driver, url)

        if result == "RESTART":
            log(f"♻️ Restarting browser for {name} (attempt {attempt})")
            driver = restart_driver(driver)
            result = scrape_tradingview(driver, url)

            if result == "RESTART":
                log("🛑 Browser restart failed again, skipping row")
                return driver, []

        if isinstance(result, list) and result:
            return driver, result

        if attempt < max_retries:
            log(f"⚠️ Empty result for {name}, retry {attempt}/{max_retries} in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        else:
            log(f"❌ All {max_retries} attempts failed for {name}")

    return driver, []

# ---------------- SHEETS ---------------- #
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

    log("❌ Batch failed after 3 attempts")
    return False

# ---------------- SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet36")

    company_list = sheet_main.col_values(7)
    name_list = sheet_main.col_values(1)

    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Loaded {len(company_list)} rows | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume from {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN ---------------- #
driver = None
batch_list = []
pending_checkpoint = last_i

attempted = 0
succeeded = 0
skipped_empty_url = 0
skipped_no_data = 0

try:
    driver = create_driver()

    for i in range(len(company_list)):
        if SHARD_STEP > 1 and (i % SHARD_STEP) != SHARD_INDEX:
            continue

        if i < last_i:
            continue

        if i >= MAX_ROWS:
            break

        if i == 0:
            log("⏭️ Skipping header row 1")
            continue

        url = company_list[i].strip() if i < len(company_list) and company_list[i] else ""
        name = name_list[i].strip() if i < len(name_list) and name_list[i] else f"Row {i+1}"

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
            log(f"📦 Buffered {len(values)} values | batch {len(batch_list)}/{BATCH_SIZE}")
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

        time.sleep(1)

finally:
    ok = flush_batch(sheet_data, batch_list)
    if ok:
        with open(checkpoint_file, "w") as f:
            f.write(str(pending_checkpoint))

    safe_quit(driver)

    log("=" * 50)
    log(f"🏁 Done | Attempted: {attempted} | Succeeded: {succeeded} | No-data: {skipped_no_data} | Empty-URL: {skipped_empty_url}")
    log("=" * 50)
