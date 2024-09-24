from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from undetected_chromedriver import Chrome, ChromeOptions
from colorama import init, Fore
import time
import random
import pandas as pd
from datetime import datetime, timezone
from alive_progress import alive_it
import random

from input_files.utils import load_items

init(autoreset=True)

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
                url = f"{item['product_link'].split('?')[0]}"
                url = f"{url}reviews" if url.endswith('/') else f"{url}/reviews"
                self._process_url(url, item)
            except KeyboardInterrupt as e:
                self.close_spider()
                print(e, False)
            
            except WebDriverException as e:
                print(e, False)
            
            except Exception as e:
                print(e, False)

    def _solve_captcha_once(self, url):
        self.driver.get(url)
        self._wait_for_captcha()
        # self.cookies = {cookie['name']: cookie['value'] for cookie in self.driver.get_cookies()}
        # print("Cookies saved after solving captcha.")

    def _wait_for_captcha(self):
        try:
            captcha = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.CheckboxCaptcha"))
            )
            if captcha:
                print("Captcha detected. Please solve it manually.")
                WebDriverWait(self.driver, 300).until_not(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.CheckboxCaptcha'))
                )
                print("Captcha solved. Resuming scraping...")
        except Exception:
            pass

    def _process_url(self, url, item):
        random.randint(1, 10)
        self.driver.get(url)
        for name, value in self.cookies.items():
            self.driver.add_cookie({'name': name, 'value': value})
        if "Нет отзывов" in self.driver.page_source:
            print(f"No reviews found for {item['product_link']}. Skipping...")
            return self._update_items(item, url_status="No Reviews")
        
        elif "На Маркете проблемы" in self.driver.page_source:
            return self._update_items(item, url_status="Not Found")
        self._parse(item)
        time.sleep(random.uniform(5, 10))

    def _find_element_by_css(self, css_selector, attribute="text", child_index=None, parent=None):
        try:
            element = WebDriverWait(parent or self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
            if attribute == "text":
                return element.text.replace('\n', ' ').strip() if child_index is None else element.find_elements(By.CSS_SELECTOR, "*")[child_index].strip()
            return element.get_attribute(attribute)
        except Exception as e:
            print(f"Error: {e}")
            return ""

    def _scroll_to_bottom(self):
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
    
    def _update_items(self, item, data_score="", data_author="", data_published="", data_content="", url_status=""):
        item['data_score'] = data_score
        item['data_author'] = data_author
        item['data_published'] = data_published
        item['data_published_parsed'] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        item['data_content'] = data_content
        item['url_status'] = url_status or "Data Extracted" if item['data_content'] else "No reviews"
        self.data.append(item)
        print(item)
        return item 
    
    def _parse(self, item):
        self._wait_for_captcha()
        self._scroll_to_bottom()
        
        time.sleep(5)
        
        
        reviews = self.driver.find_elements(By.CSS_SELECTOR, '[class="eoZns"]')
        
        if not reviews or "Ни одного отзыва" in self.driver.page_source:
            self._update_items(item, url_status="No Reviews")
            return 
        
        for review in reviews:
            data_score = len(review.find_elements(By.CSS_SELECTOR, "path[class='_1U41f _1TpzY']"))
            data_author = self._find_element_by_css("[data-auto='user_name']", parent=review)
            data_published = self._find_element_by_css("[data-auto='date_region']", parent=review)
            data_content = self._find_element_by_css("[data-auto='review-description']", parent=review)
            if data_content:
                self._update_items(item, data_score=data_score, data_author=data_author, data_published=data_published, data_content=data_content)
            else:
                self._update_items(item)
                

    def save_to_excel(self):
        df = pd.DataFrame(self.data)
        excel_filename = 'scraped_data.xlsx'
        df.to_excel(excel_filename, index=False)
        print(f"Data saved to {excel_filename}")
        
    def close_spider(self):
        self.driver.quit()
        self.save_to_excel()
        
        
if __name__ == "__main__":
    with SeleniumScraper() as scraper:
        items = [item for item in load_items()]
        # items = [{"product_link": "https://market.yandex.ru/product--43pus7406-60/997249468"}]
        scraper.start_requests(items)
