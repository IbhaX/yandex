import requests
import json
from bs4 import BeautifulSoup
from input_files.utils import load_items
from urllib.parse import urlparse
import json
import pandas as pd
import asyncio
import aiohttp
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

class YandexMarketReviews:
    def __init__(self):
        logging.info("Starting YandexMarketReviews")
        self.items = load_items()
        # self.items = [{"product_link": "https://market.yandex.ru/product--43pus7406-60/997249468"}]
        self.data = []
        self.url = "https://market.yandex.ru/api/render-lazy?w=%40card%2FReviewsLayout"
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://market.yandex.ru',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sk': 'sd279e965cb0c87f68c62576272013067',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'x-market-app-version': '2024.09.18.1-desktop.t2574117477',
            'x-market-core-service': 'default',
            'x-market-page-id': 'market:product-reviews',
            'cookie': 'i=QKtkBs6AmVyGRKgg+2tpK/T+P1YYUX4yvs/O0wULiCItRZJzZQp2Sz/IAe88ZHzJGqIPDmTjaF6axLOk8MM2jG+hgNE=; yandexuid=1704044661726667882; yashr=1570000711726667882; cmp-merge=true; reviews-merge=true; skid=833951061726805537; nec=0; muid=1152921512212072350%3A4NeHeaNYTl%2Fq6C5PngCjp52NWBwHDRJs; yuidss=1704044661726667882; ymex=2042165542.yrts.1726805542; receive-cookie-deprecation=1; _ym_uid=1726805541493289859; _ym_d=1726805542; yandexmarket=48%2CRUR%2C1%2C%2C%2C%2C2%2C0%2C0%2C213%2C0%2C0%2C12%2C0%2C0; is_gdpr=0; is_gdpr_b=CLmcHRCKlAI=; gdpr=0; global_delivery_point_skeleton={%22regionName%22:%22%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%22%2C%22addressLineWidth%22:49.400001525878906}; spvuid_market:product1414493406_expired:1727164645920=1727078245882%2F29840486050e74033a5b58bdc4220600%2F1%2F1; spvuid_market:product997249468_expired:1727186274496=1727099874453%2F47a8c9447ff261cd7f2982c6c9220600%2F1%2F1; spvuid_market:product1919720648_expired:1727194789101=1727108389055%2F5c2c5a27f8d990c527aa04c2cb220600%2F1%2F1; oq_last_shown_date=1727108599206; oq_shown_onboardings=%5B%5B%22WEB_TO_APP_nezalogin%22%2C1727108599206%2C1732378994412%5D%5D; spvuid_touch:product_expired:1727196973136=1727110572948%2Fd0f99efa9e9d85dc723b3044cc220600%2F1%2F1; js=1; visits=1726805537-1727074398-1727150305; spvuid_market:product1728631638_expired:1727236705167=1727150305113%2Fd1046299b2a9c3ed32f66884d5220600%2F1%2F1; rcrr=true; _ym_isad=2; spvuid_market:product5916644_expired:1727242518948=1727156118902%2Fa7c870725b3e0c301456f0ded6220600%2F1%2F1; spvuid_market:product1728638518_expired:1727243269300=1727156869252%2F0edffc7d3ad200636ec7a90bd7220600%2F1%2F1; parent_reqid_seq=1727156869252%2F0edffc7d3ad200636ec7a90bd7220600%2F1%2F1%2C1727157316812%2F7d0935b27452c50383005726d7220600%2F1%2F1%2C1727157358573%2F9cd5257610c573b50e37d428d7220600%2F1%2F1%2C1727157451284%2F4ae841020e7855fb39e15a2ed7220600%2F1%2F1%2C1727157871174%2Fbe4f21cc3895acedd8e46147d7220600%2F1%2F1; bh=EkEiQ2hyb21pdW0iO3Y9IjEyOCIsICJOb3Q7QT1CcmFuZCI7dj0iMjQiLCAiR29vZ2xlIENocm9tZSI7dj0iMTI4IhoFIng4NiIiECIxMjguMC42NjEzLjEzOSIqAj8wMgkiTmV4dXMgNSI6CSJXaW5kb3dzIkIIIjE1LjAuMCJKBCI2NCJSXCJDaHJvbWl1bSI7dj0iMTI4LjAuNjYxMy4xMzkiLCJOb3Q7QT1CcmFuZCI7dj0iMjQuMC4wLjAiLCJHb29nbGUgQ2hyb21lIjt2PSIxMjguMC42NjEzLjEzOSIiYPGsybcG; _yasc=9OoHxANbB9G0451beAVh4eyyT+lT4D7qwvDehIoDpS8dBXpWh2Gkv4X9RqTHV9bb+nDMRyYCUFBBtxbG; _yasc=LRheyvLQEUdcwuFfLY6EyZhhK+ngtasf+3IcWP/eR7UvKMn42sKvZt2OgCYsf3OKpRc=; i=BfCZ8+Ou85WWOiSmbtRKSssyF0qidewNsK/ivwIjsecr7HGTlPAmOn0gbRb6nVGXgiSooBYIKi32Gk3HB1L9MtXY9e8=; spravka=dD0xNjk1NTcyNjM5O2k9NTQuODYuNTAuMTM5O0Q9RjQ4MjRGRDI5NzM4QTUzREZFOEMxOUQ1NTY3N0Q3MUU5QUQzRThCQURCNjI3RUE1Mjc0MjY5MTc0NkIzQjM1NzFENTc4NzFFNTgzOUU2OTY7dT0xNjk1NTcyNjM5MjkyODE4MjQ5O2g9YjY3YTg4MGM3MDVmM2MwNDI0Zjk5NDNlNjY4YjU0YTg=; yandexuid=9997713411727158771; yashr=478096001727158771; _yasc=Gsx/IyEt72jCE4ikbjVreEk3y8lMgVSw6JsvLRowFqZeV4EOhQ6Yknf2oKS+FBiTb1trn388V/HF+7cK; i=BfCZ8+Ou85WWOiSmbtRKSssyF0qidewNsK/ivwIjsecr7HGTlPAmOn0gbRb6nVGXgiSooBYIKi32Gk3HB1L9MtXY9e8=; spravka=dD0xNjk1NTcyNjM5O2k9NTQuODYuNTAuMTM5O0Q9RjQ4MjRGRDI5NzM4QTUzREZFOEMxOUQ1NTY3N0Q3MUU5QUQzRThCQURCNjI3RUE1Mjc0MjY5MTc0NkIzQjM1NzFENTc4NzFFNTgzOUU2OTY7dT0xNjk1NTcyNjM5MjkyODE4MjQ5O2g9YjY3YTg4MGM3MDVmM2MwNDI0Zjk5NDNlNjY4YjU0YTg=; yandexuid=9997713411727158771; yashr=478096001727158771'
        }


    def set_payload(self, path, page=2):
        return {
            "widgets": [
                {
                    "lazyId": "cardReviewsLayout42",
                    "widgetName": "@card/ReviewsLayout",
                    "options": {
                        "widgetId": "reviewsPageEntitiesList",
                        "entityWrapperProps": {
                            "paddings": {
                                "top": "5",
                                "bottom": "5"
                            }
                        },
                        "nextPageConfig": "all_product_reviews_web_next_page",
                        "isChefRemixExp": False,
                        "initial": False,
                        "params": {
                            "customConfigName": "all_product_reviews_web_next_page",
                            "reviewPage": str(page),
                        },
                        "widgetSource": "default"
                    },
                    "slotOptions": {
                        "dynamic": True,
                        "measured": True
                    }
                }
            ],
            "path": f"{path}/reviews",
            "widgetsSource": "default",
            "experimental": {}
        }
        
    def handle_missing(self, item, status):
        item['url_status'] = status
        self.items.append(item)
    
    async def fetch_reviews(self, product_link, item, session):
        logging.info(f"Scraping reviews for {product_link}...")
        page = 1
        while True:
            parsed_url = urlparse(product_link)
            path = parsed_url.path
            payload = self.set_payload(path, page)
            payload = json.dumps(payload)
            async with session.post(self.url, headers=self.headers, data=payload) as response:
                logging.info(f"Response status: {response.status} for {product_link}")
                if response.status == 200:
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    reviews = soup.find_all('div', attrs={'data-auto':'review-item'})

                    if not reviews:
                        logging.info(f"No reviews found for {product_link}")
                        self.handle_missing(item, "No Reviews")
                        break
                    else:
                        for review in reviews:
                            self.parse_review(review, item)
                else:
                    logging.info(f"Failed to scrape page {page} for {product_link}. Status code: {response.status}")
                    self.handle_missing(item, "Not Found")
                    break
            
            page += 1
        
    def parse_review(self, review, item):
        author = review.select_one('meta[itemprop="author"]')['content'].strip()
        date_published = review.select_one('meta[itemprop="datePublished"]')['content'].strip()
        description = review.select_one('meta[itemprop="description"]')['content'].strip()
        rating = review.select_one('meta[itemprop="ratingValue"]')['content'].strip()
            
        item.update({
            'data_author': author,
            'date_published': date_published,
            'data_content': description,
            'data_score': rating,
            'url_status': "Data Extracted"
        })
        self.data.append(item)

    async def run(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_reviews(item['product_link'], item, session) for item in self.items]
            logging.info("Item Count: " + str(len(self.items)))
            await asyncio.gather(*tasks)
    
    def save_to_json(self):
        json_filename = 'scraped_data_api.json'
        with open(json_filename, 'w') as json_file:
            json.dump(self.data, json_file, indent=4)
        logging.info(f"Data saved to {json_filename}")
    
    def save_to_excel(self):
        df = pd.DataFrame(self.data)
        excel_filename = 'scraped_data_api.xlsx'
        df.to_excel(excel_filename, index=False)
        logging.info(f"Data saved to {excel_filename}")
    

if __name__ == "__main__":
    yandex_reviews = YandexMarketReviews()
    asyncio.run(yandex_reviews.run())
    yandex_reviews.save_to_excel()
