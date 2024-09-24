from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto("https://market.yandex.ru/product--stiralnaia-mashina-s-sushkoi-hisense-wfqa1014vjm/1919720648")
        # Perform actions on the page
        
        page_source = page.content()
        print(page_source)
        
        browser.close()

if __name__ == "__main__":
    main()
