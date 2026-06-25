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
START_COL   = "D"

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
    log("🌐 Initializing Chrome Driver...")
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
            log("🍪 Found cookies.json. Navigating to TradingView domain base to inject...")
            driver.get("https://in.tradingview.com/")
            time.sleep(3)
            
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            
            log(f"📦 Injecting {len(cookies)} cookies into the browser context...")
            for c in cookies:
                driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
            
            log("🔄 Refreshing page to apply cookies...")
            driver.refresh()
            time.sleep(5)
            log("✅ Cookies applied successfully.")
        except Exception as e:
            log(f"⚠️ Cookie error: {e}")
    else:
        log("ℹ️ No cookies.json detected. Proceeding guest mode.")
    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        log(f"🌍 VISITING URL: {url}")
        driver.get(url)
        
        target_css = "div[class^='valueValue-']" 
        
        log(f"⏳ Waiting up to 45s for base elements to appear...")
        try:
            WebDriverWait(driver, 45).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, target_css))
            )
        except TimeoutException:
            log(f"🛑 TIMEOUT HIT for {url}. Capturing visual evidence...")
            os.makedirs("screenshots", exist_ok=True)
            safe_name = url.split("symbol=")[-1].replace("%3A", "_") if "symbol=" in url else "timeout_page"
            screenshot_path = f"screenshots/{safe_name}_{int(time.time())}.png"
            
            try:
                driver.set_window_size(1920, 1080)
                driver.save_screenshot(screenshot_path)
                log(f"📸 SCREENSHOT SAVED SUCCESSFUL: {screenshot_path}")
            except Exception as ss_err:
                log(f"⚠️ Could not write screenshot to disk: {ss_err}")
                
            return "RESTART"
        
        log("📜 Executing viewport micro-scrolls...")
        driver.execute_script("window.scrollTo(0, 300);")
        time.sleep(1.5)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(3.5) 
        
        log("📸 Capturing DOM source code structure...")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        elements = soup.find_all("div", class_=lambda x: x and x.startswith("valueValue-"))
        
        # Raw pulled array containing both header quotes and indicators
        all_values = [el.get_text().replace("−", "-").replace("∅", "None").strip() for el in elements]
        
        # FILTER: The first 10 values belong to the top quote panel (price, open, high, low, volume, etc.)
        # We slice them off to keep only the technical matrix block starting from index 10.
        filtered_values = all_values[10:] if len(all_values) >= 10 else all_values
        
        log(f"📊 [DATA COLLECTION] Total Extracted: {len(all_values)} | Kept: {len(filtered_values)} values.")
        log(f"📝 [DATA ARRAY DETAILS]: {filtered_values}")
        
        return filtered_values
    except WebDriverException as e:
        log(f"⚠️ Connection issue hit: {str(e)[:100]}. Triggering browser pipeline rebuild...")
        return "RESTART"
    except Exception as e:
        log(f"💥 Parsing Failure Error: {e}")
        return []

def scrape_with_retry(driver, url, name, max_retries=MAX_RETRIES):
    for attempt in range(1, max_retries + 1):
        log(f"🔄 Processing Attempt {attempt}/{max_retries} for asset: {name}")
        result = scrape_tradingview(driver, url)
        
        if result == "RESTART":
            log(f"♻️ Restarting browser instance entirely (Attempt {attempt})")
            try: 
                driver.quit()
            except: 
                pass
            driver = create_driver()
            continue
            
        if isinstance(result, list) and len(result) > 0:
            return driver, result
            
        log(f"⚠️ Attempt {attempt} returned empty data array for {name}")
        if attempt < max_retries:
            log(f"💤 Sleeping {RETRY_DELAY}s before next retry sweep...")
            time.sleep(RETRY_DELAY)
            
    log(f"❌ All {max_retries} attempts exhausted. Moving on with empty payload.")
    return driver, []

# ---------------- MAIN LOOP ---------------- #
log("🔗 Authenticating with Google Sheets API...")
gc = gspread.service_account("credentials.json")
sheet_main = gc.open("Stock List").worksheet("Sheet1")
sheet_data = gc.open("MV2 for SQL").worksheet("Sheet36")

log("Reading configuration columns from source file...")
company_list = sheet_main.col_values(7)
name_list = sheet_main.col_values(1)
log(f"📋 Loaded {len(company_list)} potential target addresses.")

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
        log(f"⏭️ Skipping Row {i+1}: Missing link information.")
        continue

    log(f"\n{'-'*30}\n🚀 PROCESSING ROW {i+1}: {name}\n{'-'*30}")
    driver, values = scrape_with_retry(driver, url, name)

    if values:
        target_range = f"{START_COL}{i+1}"
        log(f"💾 Staging values to write to range {target_range}")
        batch_list.append({"range": target_range, "values": [values]})
    else:
        log(f"🛑 No data parsed or written for row {i+1} ({name})")
    
    if len(batch_list) >= BATCH_SIZE:
        log(f"📤 [BATCH LIMIT REACHED] Sending {len(batch_list)} records to Google Sheets...")
        sheet_data.batch_update(batch_list)
        log("✨ Batch sync update completed successfully.")
        batch_list = []
    
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))

if batch_list:
    log(f"📤 [FINAL SYNC] Flushing remaining {len(batch_list)} database items...")
    sheet_data.batch_update(batch_list)
    log("✨ Final flush complete.")

driver.quit()
log("🏁 Execution pipeline completed. Done.")
