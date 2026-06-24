import sys
import os
import time
import json
import random
import logging
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
import gspread
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- LOGGING CONFIG ---------------- #
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 18
BATCH_SIZE = 50
RESTART_EVERY_ROWS = 10
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()
DAY_OUTPUT_START_COL = 3

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DAY_END_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)
STATUS_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT)
SHEET_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1)
BROWSER_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2)

def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            logger.warning(f"API Issue: {str(e)[:50]}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    return func(*args, **kwargs)

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    logger.info(f"Initializing stealth browser [Shard {SHARD_INDEX}]...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    
    stealth(drv,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    
    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                try: drv.add_cookie(c)
                except: continue
            drv.refresh()
            time.sleep(2)
        except: pass
    return drv

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def restart_driver():
    global driver
    if driver:
        try: driver.quit()
        except: pass
    driver = None

def get_values(drv):
    try:
        wait = WebDriverWait(drv, 20)

        # Wait for at least something to load
        wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "[class*='valueValue']")
            )
        )

        elements = drv.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")

        values = []
        seen = set()

        for el in elements:
            try:
                if not el.is_displayed():
                    continue

                text = el.text.strip()

                if not text:
                    continue

                # avoid duplicates
                if text in seen:
                    continue

                seen.add(text)
                values.append(text)

            except:
                continue

        # STRICT LIMIT (important)
        values = values[:EXPECTED_COUNT]

        logger.info(f"RAW FOUND: {len(elements)} | FINAL: {len(values)}")
        logger.info(f"VALUES: {values}")

        return values

    except Exception as e:
        logger.error(f"get_values error: {e}")
        return []
def scrape_day(url):
    if not url: return [""] * EXPECTED_COUNT, "NOT OK", "", ""
    drv = ensure_driver()
    try:
        logger.info(f"Visiting: {url}")
        drv.get(url)
        time.sleep(random.uniform(4, 6))
        
        vals = get_values(drv)
        if len(vals) < EXPECTED_COUNT:
            drv.execute_script("window.scrollTo(0, 500);")
            time.sleep(2)
            vals = get_values(drv)

        status = "OK" if len(vals) >= EXPECTED_COUNT else "NOT OK"
        logger.info(f"Result: {status} | Found {len(vals)}/{EXPECTED_COUNT} values: {vals}")
        
        return (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT], status, url, drv.current_url
    except Exception as e:
        logger.error(f"Failed to scrape {url}: {e}")
        restart_driver()
        return [""] * EXPECTED_COUNT, "NOT OK", url, ""

# ---------------- MAIN ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    return gc.open("STOCKLIST 2").worksheet("Sheet1"), gc.open("MV2 for SQL").worksheet("Sheet36")

sheet_main, sheet_data = connect_sheets()
company_list = api_retry(sheet_main.col_values, 1)
url_list = api_retry(sheet_main.col_values, 7)

last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else START_ROW
current_date = date.today().strftime("%m/%d/%Y")
batch_list = []

for i in range(last_i, min(END_ROW, len(company_list))):
    name = company_list[i]
    url = url_list[i] if i < len(url_list) else None
    
    vals, status, s_url, b_url = scrape_day(url)
    
    row_idx = i + 1
    batch_list.extend([
        {"range": f"A{row_idx}", "values": [[name]]},
        {"range": f"B{row_idx}", "values": [[current_date]]},
        {"range": f"{DAY_START_COL_LETTER}{row_idx}:{DAY_END_COL_LETTER}{row_idx}", "values": [vals]},
        {"range": f"{STATUS_COL}{row_idx}", "values": [[status]]},
        {"range": f"{SHEET_URL_COL}{row_idx}", "values": [[s_url]]},
        {"range": f"{BROWSER_URL_COL}{row_idx}", "values": [[b_url]]}
    ])

    with open(checkpoint_file, "w") as f: f.write(str(i + 1))
    if (i + 1) % RESTART_EVERY_ROWS == 0: restart_driver()
    
    if len(batch_list) >= (BATCH_SIZE * 6):
        api_retry(sheet_data.batch_update, batch_list)
        batch_list = []

if batch_list: api_retry(sheet_data.batch_update, batch_list)
restart_driver()
logger.info("Scraping completed.")
