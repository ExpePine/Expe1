import json
from selenium import webdriver

driver = webdriver.Chrome()

driver.get("https://blinkit.com")

with open("cookies.json") as f:
    cookies = json.load(f)

for cookie in cookies:
    driver.add_cookie(cookie)

driver.refresh()

driver.get("https://blinkit.com/account/orders")

# scrape orders
