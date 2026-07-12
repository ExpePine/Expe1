import os
import json
import time
from pathlib import Path
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BLINKIT_HOME = "https://blinkit.com/"
BLINKIT_ORDERS = "https://blinkit.com/account/orders"
OUTPUT_DIR = Path("blinkit_output")
OUTPUT_DIR.mkdir(exist_ok=True)

def log(message):
    print(f"[BLINKIT] {message}", flush=True)

def create_driver():
    log("Initializing Undetected Chrome driver...")
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Launching with specific window size
    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

def load_cookie_json():
    raw_cookies = os.getenv("BLINKIT_COOKIES_JSON", "").strip()
    if not raw_cookies:
        raise RuntimeError("BLINKIT_COOKIES_JSON secret is missing.")
    cookies = json.loads(raw_cookies)
    return cookies["cookies"] if isinstance(cookies, dict) and "cookies" in cookies else cookies

def add_blinkit_cookies(driver, cookies):
    driver.get(BLINKIT_HOME)
    time.sleep(5) # Allow site to load
    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
        except Exception as e:
            log(f"Skipping cookie: {e}")
    driver.refresh()
    time.sleep(5)

def fetch_orders(driver):
    log("Navigating to orders...")
    driver.get(BLINKIT_ORDERS)
    # Wait for the main container that holds order history
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(5) # Extra buffer for dynamic content
    except Exception:
        log("Timeout waiting for page load.")

    # Save outputs
    (OUTPUT_DIR / "blinkit_orders.html").write_text(driver.page_source, encoding="utf-8")
    page_text = driver.find_element(By.TAG_NAME, "body").text
    (OUTPUT_DIR / "blinkit_orders.txt").write_text(page_text, encoding="utf-8")
    log("Order content saved successfully.")

def main():
    driver = None
    try:
        cookies = load_cookie_json()
        driver = create_driver()
        add_blinkit_cookies(driver, cookies)
        fetch_orders(driver)
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
