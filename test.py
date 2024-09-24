
import scrapy

class MySpider(scrapy.Spider):
    name = 'test'
    start_urls = ['https://market.yandex.ru/product--vc53001mrnt/1728638518']
    
    custom_settings = {
      "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    }

    def parse(self, response):
        # Extract data from the page
        reviews = response.css("span._2N6fd::text").get()

        # Yield the extracted data
        yield {
            'reviews': reviews
        }

