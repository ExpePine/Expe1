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
MAX_RETRIES = 3         # retries per URL before giving up
RETRY_DELAY = 3         # seconds between retries
BATCH_SIZE  = 50
START_COL   = "DH"      # first column to write scraped values

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

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
def save_debug(driver, prefix):
    try:
        ts = int(time.time())
        screenshot_file = f"{prefix}_{ts}.png"
        html_file = f"{prefix}_{ts}.html"
        driver.save_screenshot(screenshot_file)
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        log(f"📸 Saved screenshot: {screenshot_file}")
        log(f"💾 Saved HTML: {html_file}")
    except Exception as e:
        log(f"⚠️ Debug save failed: {e}")


def scrape_tradingview(driver, url):
    try:
        log("🌍 VISITING URL: " + url)
        driver.get(url)
        WebDriverWait(driver, 45).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.valueValue-l31H9iuA"))
        )
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        elements = soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        
        values = []
        for el in elements:
            val = el.get_text().replace("−", "-").replace("∅", "None").strip()
            values.append(val)
        
        return values
    except Exception as e:
        log(f"Error scraping {url}: {e}")
        return []

def scrape_with_retry(driver, url, name, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        values = scrape_tradingview(driver, url)
        if values:
            return driver, values
        log(f"⚠️ Attempt {attempt} failed for {name}")
        time.sleep(RETRY_DELAY)
    return driver, []


# ---------------- MAIN LOOP ---------------- #
gc = gspread.service_account("credentials.json")
sheet_main = gc.open("Stock List").worksheet("Sheet1")
sheet_data = gc.open("MV2 for SQL").worksheet("Sheet36")

company_list = sheet_main.col_values(7)
name_list = sheet_main.col_values(1)

driver = create_driver()
batch_list = []
attempted = 0
succeeded = 0

for i in range(len(company_list)):
    if SHARD_STEP > 1 and (i % SHARD_STEP) != SHARD_INDEX:
        continue
    if i < last_i or i == 0:
        continue

    url = company_list[i].strip() if i < len(company_list) else ""
    name = name_list[i].strip() if i < len(name_list) else f"Row {i+1}"

    if not url:
        continue

    log(f"\n--- Processing Row {i+1}: {name} ---")
    driver, values = scrape_with_retry(driver, url, name)

    if values:
        log(f"✅ Captured {len(values)} values:")
        for idx, val in enumerate(values, start=1):
            log(f"   [{idx}] {val}")
        
        batch_list.append({
            "range": f"{START_COL}{i+1}",
            "values": [values]
        })
        succeeded += 1
    
    if len(batch_list) >= BATCH_SIZE:
        sheet_data.batch_update(batch_list)
        batch_list = []
    
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))

driver.quit()
