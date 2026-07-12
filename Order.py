import os
import json
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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
    options = Options()

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    options.add_argument(
        "--user-agent=Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    )

    return webdriver.Chrome(options=options)


def load_cookie_json():
    raw_cookies = os.getenv("BLINKIT_COOKIES_JSON", "").strip()

    if not raw_cookies:
        raise RuntimeError(
            "BLINKIT_COOKIES_JSON GitHub Secret is missing."
        )

    try:
        cookies = json.loads(raw_cookies)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"Invalid BLINKIT_COOKIES_JSON: {error}"
        )

    # Support common browser cookie export format:
    # {"cookies": [...]}
    if isinstance(cookies, dict) and "cookies" in cookies:
        cookies = cookies["cookies"]

    if not isinstance(cookies, list):
        raise RuntimeError(
            "Cookie JSON must be a JSON array or contain a 'cookies' array."
        )

    return cookies


def normalize_cookie(cookie):
    allowed_keys = {
        "name",
        "value",
        "path",
        "domain",
        "secure",
        "httpOnly",
        "sameSite",
        "expiry",
    }

    clean_cookie = {}

    for key in allowed_keys:
        if key in cookie:
            clean_cookie[key] = cookie[key]

    if "expirationDate" in cookie and "expiry" not in clean_cookie:
        try:
            clean_cookie["expiry"] = int(cookie["expirationDate"])
        except Exception:
            pass

    if "expiry" in clean_cookie:
        try:
            clean_cookie["expiry"] = int(clean_cookie["expiry"])
        except Exception:
            clean_cookie.pop("expiry", None)

    same_site = clean_cookie.get("sameSite")

    if same_site:
        same_site_map = {
            "no_restriction": "None",
            "unspecified": "Lax",
            "lax": "Lax",
            "strict": "Strict",
            "none": "None",
        }

        clean_cookie["sameSite"] = same_site_map.get(
            str(same_site).lower(),
            same_site
        )

        if clean_cookie["sameSite"] not in ["Strict", "Lax", "None"]:
            clean_cookie.pop("sameSite", None)

    return clean_cookie


def add_blinkit_cookies(driver, cookies):
    log("Opening Blinkit domain...")

    driver.get(BLINKIT_HOME)

    time.sleep(5)

    added = 0
    failed = 0

    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue

        cookie_name = cookie.get("name", "UNKNOWN")

        try:
            clean_cookie = normalize_cookie(cookie)

            if not clean_cookie.get("name"):
                continue

            if "value" not in clean_cookie:
                continue

            # Selenium can reject incompatible exported domains.
            domain = clean_cookie.get("domain", "")

            if domain and "blinkit.com" not in domain:
                log(f"Skipping non-Blinkit cookie: {cookie_name}")
                continue

            try:
                driver.add_cookie(clean_cookie)

            except Exception:
                # Retry without domain/sameSite because browser exports
                # sometimes contain Selenium-incompatible values.
                clean_cookie.pop("domain", None)
                clean_cookie.pop("sameSite", None)

                driver.add_cookie(clean_cookie)

            added += 1

        except Exception as error:
            failed += 1
            log(
                f"Cookie failed: {cookie_name} | "
                f"{type(error).__name__}"
            )

    log(f"Cookies added: {added}")
    log(f"Cookies failed: {failed}")

    if added == 0:
        raise RuntimeError(
            "No Blinkit cookies were added successfully."
        )


def save_debug_files(driver):
    screenshot_path = OUTPUT_DIR / "blinkit_orders.png"
    html_path = OUTPUT_DIR / "blinkit_orders.html"

    driver.save_screenshot(str(screenshot_path))

    html_path.write_text(
        driver.page_source,
        encoding="utf-8"
    )

    log(f"Screenshot saved: {screenshot_path}")
    log(f"HTML saved: {html_path}")


def fetch_orders(driver):
    log("Opening Blinkit orders page...")

    driver.get(BLINKIT_ORDERS)

    time.sleep(10)

    log(f"Current URL: {driver.current_url}")
    log(f"Page title: {driver.title}")

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.TAG_NAME, "body")
            )
        )
    except Exception:
        log("Body wait timed out.")

    save_debug_files(driver)

    body = driver.find_element(By.TAG_NAME, "body")

    page_text = body.text.strip()

    text_path = OUTPUT_DIR / "blinkit_orders.txt"

    text_path.write_text(
        page_text,
        encoding="utf-8"
    )

    log("VISIBLE PAGE TEXT")
    log("=" * 80)

    print(page_text, flush=True)

    log("=" * 80)

    lower_text = page_text.lower()

    login_indicators = [
        "login",
        "log in",
        "continue with phone",
        "enter mobile number",
    ]

    if any(
        indicator in lower_text
        for indicator in login_indicators
    ):
        raise RuntimeError(
            "Blinkit cookie session appears expired or login is required."
        )

    log(f"Order page text saved: {text_path}")


def main():
    driver = None

    try:
        log("Starting Blinkit order fetcher...")

        cookies = load_cookie_json()

        log(f"Cookies received: {len(cookies)}")

        driver = create_driver()

        add_blinkit_cookies(
            driver,
            cookies
        )

        log("Refreshing session...")

        driver.refresh()

        time.sleep(5)

        fetch_orders(driver)

        log("Blinkit order fetch completed successfully.")

    except Exception as error:
        log("=" * 80)
        log("SCRIPT FAILED")
        log(f"Error type: {type(error).__name__}")
        log(f"Error: {error}")
        log("=" * 80)

        if driver:
            try:
                save_debug_files(driver)
            except Exception:
                pass

        raise

    finally:
        if driver:
            driver.quit()

        log("Browser closed.")


if __name__ == "__main__":
    main()
