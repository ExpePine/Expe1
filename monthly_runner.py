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
from selenium.common.exceptions import WebDriverException, TimeoutException

from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG ---------------- #
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

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(60)
    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    log(f"🔗 Visiting: {url}")
    try:
        driver.get(url)
        # Wait for the table cells to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='valueValue']"))
        )
        time.sleep(2)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Update this class selector if the layout changes
        elements = soup.find_all("div", class_="valueValue-l31H9iuA")
        values = [el.get_text().strip() for el in elements]
        
        if not values:
            log("⚠️ Page loaded but no data found with current selector.")
        return values
    except Exception as e:
        log(f"❌ Error scraping {url}: {e}")
        return []

# ---------------- MAIN LOOP ---------------- #
gc = gspread.service_account("credentials.json")
sheet_main = gc.open("Stock List").worksheet("Sheet1")
sheet_data = gc.open("MV2 for SQL").worksheet("Sheet36")

company_list = sheet_main.col_values(7)
name_list    = sheet_main.col_values(1)
driver = create_driver()
batch_list = []

try:
    for i in range(len(company_list)):
        if SHARD_STEP > 1 and (i % SHARD_STEP) != SHARD_INDEX: continue
        if i < last_i or i == 0 or not company_list[i].strip(): continue
        
        url = company_list[i].strip()
        log(f"🔍 [{i+1}] Processing: {name_list[i]}")
        
        values = scrape_tradingview(driver, url)
        
        if values:
            batch_list.append({"range": f"{START_COL}{i+1}", "values": [values]})
        
        if len(batch_list) >= BATCH_SIZE:
            sheet_data.batch_update(batch_list)
            batch_list = []
            
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        time.sleep(1)

finally:
    if batch_list: sheet_data.batch_update(batch_list)
    driver.quit()
    log("🏁 Process Finished.")
