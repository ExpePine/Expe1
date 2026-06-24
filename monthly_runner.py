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
MAX_RETRIES = 3
RETRY_DELAY = 5
BATCH_SIZE  = 50
START_COL   = "DH"

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
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(60)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
            driver.refresh()
            time.sleep(3)
            log("✅ Cookies applied")
        except Exception as e:
            log(f"⚠️ Cookie error: {e}")
    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        log(f"🌍 VISITING URL: {url}")
        driver.get(url)
        WebDriverWait(driver, 45).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.valueValue-l31H9iuA"))
        )
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        elements = soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        
        values = [el.get_text().replace("−", "-").replace("∅", "None").strip() for el in elements]
        return values
    except (WebDriverException, TimeoutException):
        return "RESTART"
    except Exception as e:
        log(f"💥 Error: {e}")
        return []

def scrape_with_retry(driver, url, name, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        result = scrape_tradingview(driver, url)
        
        if result == "RESTART":
            log(f"♻️ Restarting browser (Attempt {attempt})")
            try: driver.quit()
            except: pass
            driver = create_driver()
            continue
            
        if isinstance(result, list) and len(result) > 0:
            return driver, result
            
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
        log(f"📊 VALUES RECEIVED for {name}: {values}")
        batch_list.append({"range": f"{START_COL}{i+1}", "values": [values]})
    
    if len(batch_list) >= BATCH_SIZE:
        sheet_data.batch_update(batch_list)
        batch_list = []
    
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))

if batch_list:
    sheet_data.batch_update(batch_list)
driver.quit()
log("🏁 Done.")
