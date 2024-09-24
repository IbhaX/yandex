import json
import logging
import random
import time
from functools import lru_cache
from typing import List

import pandas as pd
from alive_progress import alive_it
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from undetected_chromedriver import Chrome, ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager

from input_files.utils import load_items

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@lru_cache()
def load_json():
    with open("scraped_data_api.json", 'r') as json_file:
        data = json.load(json_file)
    return data

class SeleniumScraper:
    def __init__(self):
        self.driver = self._initialize_driver()
        self.cookies = {}
        self.data = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_spider()

    def _initialize_driver(self):
        chrome_options = ChromeOptions()
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36')
        return Chrome(driver_executable_path=ChromeDriverManager().install(), options=chrome_options)

    def start_requests(self, items):
        self._solve_captcha_once(items[0]["product_link"])
        input("Press Enter to continue after solving the captcha...")
        for item in alive_it(items):
            try:
                url = item['product_link'].split('?')[0]
                self._process_url(url, item)
            except KeyboardInterrupt as e:
                logger.error(f"KeyboardInterrupt: {e}")
                self.close_spider()
            except WebDriverException as e:
                logger.error(f"WebDriverException: {e}")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")

    def _solve_captcha_once(self, url):
        self.driver.get(url)
        self._wait_for_captcha()

    def _wait_for_captcha(self):
        try:
            captcha = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.CheckboxCaptcha"))
            )
            if captcha:
                logger.info("Captcha detected. Please solve it manually.")
                WebDriverWait(self.driver, 300).until_not(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.CheckboxCaptcha'))
                )
                logger.info("Captcha solved. Resuming scraping...")
        except Exception:
            logger.info("No captcha detected.")


    def _process_url(self, url, item):
        random.randint(1, 10)
        self.driver.get(url)
        for name, value in self.cookies.items():
            self.driver.add_cookie({'name': name, 'value': value})

        self._wait_for_captcha()  # Always check for captcha

        if "Нет отзывов" in self.driver.page_source:
            logger.info(f"No reviews found for {item['product_link']}. Skipping...")
            return

        elif "На Маркете проблемы" in self.driver.page_source:
            logger.info(f"Market problems for {item['product_link']}. Skipping...")
            return

        else:
            self.data.append(item["product_link"])
            logger.info(f"Processed {item['product_link']}")

        time.sleep(random.uniform(5, 10))

    def _find_element_by_css(self, css_selector, attribute="text", child_index=None, parent=None):
        try:
            element = WebDriverWait(parent or self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
            if attribute == "text":
                return element.text.replace('\n', ' ').strip() if child_index is None else element.find_elements(By.CSS_SELECTOR, "*")[child_index].strip()
            return element.get_attribute(attribute)
        except Exception as e:
            logger.error(f"Error finding element: {e}")
            return ""

    def save_to_json(self):
        json_filename = 'cleaned.json'
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=4)
        logger.info(f"Data saved to {json_filename}")

    def close_spider(self):
        self.driver.quit()
        self.save_to_json()
        logger.info("Spider closed.")

if __name__ == "__main__":
    with SeleniumScraper() as scraper:
        items = load_items()
        scraper.start_requests(items)
