import os
import json
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ============================================================
# CONFIGURATION
# ============================================================

BLINKIT_HOME = "https://blinkit.com/"
BLINKIT_ORDERS = "https://blinkit.com/account/orders"

OUTPUT_DIR = Path("blinkit_output")
OUTPUT_DIR.mkdir(exist_ok=True)


# ============================================================
# LOGGER
# ============================================================

def log(message):
    print(f"[BLINKIT] {message}", flush=True)


# ============================================================
# CREATE CHROME DRIVER
# ============================================================

def create_driver():

    log("Creating Chrome driver...")

    options = Options()

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    options.add_argument("--window-size=1920,1080")

    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")

    options.add_argument(
        "--user-agent=Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(60)

    log("Chrome driver created.")

    return driver


# ============================================================
# LOAD COOKIE JSON
# ============================================================

def load_cookie_json():

    log("Loading Blinkit cookies...")

    raw_cookies = os.getenv(
        "BLINKIT_COOKIES_JSON",
        ""
    ).strip()

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

    # Support:
    #
    # [
    #   {...},
    #   {...}
    # ]
    #
    # AND
    #
    # {
    #   "cookies": [...]
    # }

    if isinstance(cookies, dict):

        if "cookies" in cookies:

            cookies = cookies["cookies"]

    if not isinstance(cookies, list):

        raise RuntimeError(
            "Cookie JSON must be a JSON array."
        )

    log(
        f"Cookies received: {len(cookies)}"
    )

    return cookies


# ============================================================
# NORMALIZE COOKIE
# ============================================================

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


    # Convert expirationDate

    if (
        "expirationDate" in cookie
        and
        "expiry" not in clean_cookie
    ):

        try:

            clean_cookie["expiry"] = int(
                cookie["expirationDate"]
            )

        except Exception:

            pass


    # Fix expiry

    if "expiry" in clean_cookie:

        try:

            clean_cookie["expiry"] = int(
                clean_cookie["expiry"]
            )

        except Exception:

            clean_cookie.pop(
                "expiry",
                None
            )


    # Fix SameSite

    same_site = clean_cookie.get(
        "sameSite"
    )

    if same_site:

        same_site_map = {

            "no_restriction": "None",
            "unspecified": "Lax",
            "lax": "Lax",
            "strict": "Strict",
            "none": "None",

        }

        same_site = same_site_map.get(

            str(same_site).lower(),

            same_site

        )

        if same_site in [

            "Strict",
            "Lax",
            "None",

        ]:

            clean_cookie["sameSite"] = same_site

        else:

            clean_cookie.pop(
                "sameSite",
                None
            )

    return clean_cookie


# ============================================================
# ADD COOKIES
# ============================================================

def add_blinkit_cookies(
    driver,
    cookies
):

    log("Opening Blinkit domain...")

    driver.get(
        BLINKIT_HOME
    )

    time.sleep(5)

    added = 0

    failed = 0

    skipped = 0


    for cookie in cookies:

        if not isinstance(
            cookie,
            dict
        ):

            skipped += 1

            continue


        cookie_name = cookie.get(
            "name",
            "UNKNOWN"
        )


        try:

            clean_cookie = normalize_cookie(
                cookie
            )


            if not clean_cookie.get(
                "name"
            ):

                skipped += 1

                continue


            if "value" not in clean_cookie:

                skipped += 1

                continue


            domain = clean_cookie.get(
                "domain",
                ""
            )


            if (
                domain
                and
                "blinkit.com" not in domain
            ):

                log(
                    f"Skipping non-Blinkit cookie: "
                    f"{cookie_name}"
                )

                skipped += 1

                continue


            try:

                driver.add_cookie(
                    clean_cookie
                )


            except Exception:

                # Browser exported cookies can
                # contain unsupported domain
                # or SameSite values.

                clean_cookie.pop(
                    "domain",
                    None
                )

                clean_cookie.pop(
                    "sameSite",
                    None
                )

                driver.add_cookie(
                    clean_cookie
                )


            added += 1


        except Exception as error:

            failed += 1

            log(
                f"Cookie failed: "
                f"{cookie_name} | "
                f"{type(error).__name__}: "
                f"{error}"
            )


    log(
        f"Cookies added: {added}"
    )

    log(
        f"Cookies failed: {failed}"
    )

    log(
        f"Cookies skipped: {skipped}"
    )


    if added == 0:

        raise RuntimeError(
            "No Blinkit cookies were added."
        )


# ============================================================
# SAVE DEBUG FILES
# ============================================================

def save_debug_files(driver):

    log("Saving debug files...")

    screenshot_path = (
        OUTPUT_DIR
        /
        "blinkit_orders.png"
    )

    html_path = (
        OUTPUT_DIR
        /
        "blinkit_orders.html"
    )


    try:

        driver.save_screenshot(
            str(screenshot_path)
        )

        log(
            f"Screenshot saved: "
            f"{screenshot_path}"
        )

    except Exception as error:

        log(
            f"Screenshot error: {error}"
        )


    try:

        html_path.write_text(

            driver.page_source,

            encoding="utf-8"

        )

        log(
            f"HTML saved: "
            f"{html_path}"
        )

    except Exception as error:

        log(
            f"HTML save error: {error}"
        )


# ============================================================
# DETECT BLOCK PAGE
# ============================================================

def detect_block_page(
    page_text,
    page_title
):

    combined_text = (

        page_text.lower()
        +
        " "
        +
        page_title.lower()

    )


    blocked_indicators = [

        "access denied",

        "you have been blocked",

        "sorry, you have been blocked",

        "cloudflare ray id",

        "ray id",

        "blinkit | error page",

    ]


    for indicator in blocked_indicators:

        if indicator in combined_text:

            log(
                f"BLOCK DETECTED: {indicator}"
            )

            return True


    return False


# ============================================================
# DETECT LOGIN PAGE
# ============================================================

def detect_login_page(
    page_text
):

    lower_text = page_text.lower()


    login_indicators = [

        "continue with phone",

        "enter mobile number",

        "enter phone number",

        "verify otp",

    ]


    for indicator in login_indicators:

        if indicator in lower_text:

            log(
                f"LOGIN DETECTED: {indicator}"
            )

            return True


    return False


# ============================================================
# FETCH ORDERS
# ============================================================

def fetch_orders(driver):

    log(
        "Opening Blinkit orders page..."
    )


    driver.get(
        BLINKIT_ORDERS
    )


    time.sleep(10)


    current_url = driver.current_url

    page_title = driver.title


    log(
        f"Current URL: {current_url}"
    )

    log(
        f"Page title: {page_title}"
    )


    try:

        WebDriverWait(
            driver,
            20
        ).until(

            EC.presence_of_element_located(

                (
                    By.TAG_NAME,
                    "body"
                )

            )

        )


    except Exception:

        log(
            "Body wait timed out."
        )


    # Save screenshot and HTML

    save_debug_files(
        driver
    )


    body = driver.find_element(

        By.TAG_NAME,

        "body"

    )


    page_text = body.text.strip()


    text_path = (

        OUTPUT_DIR
        /
        "blinkit_orders.txt"

    )


    text_path.write_text(

        page_text,

        encoding="utf-8"

    )


    log(
        "VISIBLE PAGE TEXT"
    )

    log(
        "=" * 80
    )


    print(
        page_text,
        flush=True
    )


    log(
        "=" * 80
    )


    # ========================================================
    # CHECK BLOCK
    # ========================================================

    if detect_block_page(

        page_text,

        page_title

    ):

        raise RuntimeError(

            "Blinkit blocked this runner IP. "
            "The orders page cannot be accessed "
            "from the current GitHub runner."

        )


    # ========================================================
    # CHECK LOGIN
    # ========================================================

    if detect_login_page(
        page_text
    ):

        raise RuntimeError(

            "Blinkit cookie session is expired "
            "or login is required."

        )


    # ========================================================
    # CHECK EMPTY PAGE
    # ========================================================

    if not page_text:

        raise RuntimeError(

            "Blinkit returned an empty page."

        )


    log(
        f"Order page text saved: "
        f"{text_path}"
    )


    log(
        "Blinkit order page loaded successfully."
    )


# ============================================================
# MAIN
# ============================================================

def main():

    driver = None


    try:

        log(
            "Starting Blinkit order fetcher..."
        )


        # Load cookies

        cookies = load_cookie_json()


        # Start Chrome

        driver = create_driver()


        # Add cookies

        add_blinkit_cookies(

            driver,

            cookies

        )


        # Refresh session

        log(
            "Refreshing Blinkit session..."
        )


        driver.refresh()


        time.sleep(5)


        # Fetch orders

        fetch_orders(
            driver
        )


        log(
            "=" * 80
        )

        log(
            "BLINKIT ORDER FETCH SUCCESSFUL"
        )

        log(
            "=" * 80
        )


    except Exception as error:

        log(
            "=" * 80
        )

        log(
            "SCRIPT FAILED"
        )

        log(
            f"Error type: "
            f"{type(error).__name__}"
        )

        log(
            f"Error: {error}"
        )

        log(
            "=" * 80
        )


        if driver:

            try:

                save_debug_files(
                    driver
                )

            except Exception:

                pass


        # IMPORTANT
        # Makes GitHub Action FAILED

        raise


    finally:

        if driver:

            driver.quit()


        log(
            "Browser closed."
        )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    main()
