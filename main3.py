import requests
from bs4 import BeautifulSoup
import tls_client
import json
import pandas as pd
import datetime
from pathlib import Path
from input_files.utils import load_items

session = tls_client.Session(
    client_identifier="chrome112",
    random_tls_extension_order=True
)

proxy_url = "http://geonode_SxA5BnHLFX-country-RU:6aaed733-56f0-4ffc-9b44-b7cc64c5ca87@mixed-unlimited.geonode.com:9004"

headers = {
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'en-US,en;q=0.9',
  'cache-control': 'max-age=0',
  'cookie': 'i=QKtkBs6AmVyGRKgg+2tpK/T+P1YYUX4yvs/O0wULiCItRZJzZQp2Sz/IAe88ZHzJGqIPDmTjaF6axLOk8MM2jG+hgNE=; yandexuid=1704044661726667882; yashr=1570000711726667882; cmp-merge=true; reviews-merge=true; skid=833951061726805537; nec=0; muid=1152921512212072350%3A4NeHeaNYTl%2Fq6C5PngCjp52NWBwHDRJs; yuidss=1704044661726667882; ymex=2042165542.yrts.1726805542; receive-cookie-deprecation=1; _ym_uid=1726805541493289859; _ym_d=1726805542; yandexmarket=48%2CRUR%2C1%2C%2C%2C%2C2%2C0%2C0%2C213%2C0%2C0%2C12%2C0%2C0; is_gdpr=0; is_gdpr_b=CLmcHRCKlAI=; visits=1726805537-1726805537-1727074398; gdpr=0; global_delivery_point_skeleton={%22regionName%22:%22%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%22%2C%22addressLineWidth%22:49.400001525878906}; spvuid_market:product1414493406_expired:1727164645920=1727078245882%2F29840486050e74033a5b58bdc4220600%2F1%2F1; _ym_isad=2; spvuid_market:product997249468_expired:1727186274496=1727099874453%2F47a8c9447ff261cd7f2982c6c9220600%2F1%2F1; rcrr=true; oq_last_shown_date=1727104055137; parent_reqid_seq=1727103213369%2F346648ed24e1186af2f8858dca220600%2F1%2F1%2C1727103218049%2Fcd22e000d9787af71b63cd8dca220600%2F1%2F1%2C1727103409231%2Fd5fe35bf869e40d32b983299ca220600%2F1%2F1%2C1727104051060%2Ffe9e833609d796ae9d1f74bfca220600%2F1%2F1%2C1727108389055%2F5c2c5a27f8d990c527aa04c2cb220600%2F1%2F1; oq_shown_onboardings=%5B%5B%22WEB_TO_APP_nezalogin%22%2C1727104055137%2C1732374451258%5D%5D; spvuid_market:product1919720648_expired:1727194789101=1727108389055%2F5c2c5a27f8d990c527aa04c2cb220600%2F1%2F1; _yasc=91jqcAd8Y+7Z33phly/nUqBwJcTR58yAEN1cZTkckkoeWJyBHsq7+zDc3q+DwwxHXp52H2zF0N4NDeIY; bh=EkEiQ2hyb21pdW0iO3Y9IjEyOCIsICJOb3Q7QT1CcmFuZCI7dj0iMjQiLCAiR29vZ2xlIENocm9tZSI7dj0iMTI4IhoFIng4NiIiECIxMjguMC42NjEzLjEzOSIqAj8xMgkiTmV4dXMgNSI6CSJBbmRyb2lkIkIIIjE1LjAuMCJKBCI2NCJSXCJDaHJvbWl1bSI7dj0iMTI4LjAuNjYxMy4xMzkiLCJOb3Q7QT1CcmFuZCI7dj0iMjQuMC4wLjAiLCJHb29nbGUgQ2hyb21lIjt2PSIxMjguMC42NjEzLjEzOSIiYOyrxrcGaiPcyqXsBs+fjJ8FrKe8uwWgnezrA/y5r/8H3/3bmQLltc2HCA==; _yasc=IajkVm+Idvna+QhBZAtk9A+EcStkWvraybqQkE2YRN0s6HAYjvPGNeyv1lBLZnu8CY6OmQg7aLmfy58zlkw=; spravka=dD0xNjk1NTcyNjM5O2k9NTQuODYuNTAuMTM5O0Q9RjQ4MjRGRDI5NzM4QTUzREZFOEMxOUQ1NTY3N0Q3MUU5QUQzRThCQURCNjI3RUE1Mjc0MjY5MTc0NkIzQjM1NzFENTc4NzFFNTgzOUU2OTY7dT0xNjk1NTcyNjM5MjkyODE4MjQ5O2g9YjY3YTg4MGM3MDVmM2MwNDI0Zjk5NDNlNjY4YjU0YTg=',
  'priority': 'u=0, i',
  'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
  'sec-ch-ua-mobile': '?1',
  'sec-ch-ua-platform': '"Android"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'none',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Mobile Safari/537.36'
}

dataset = []

def add_slash_to_last(string):
    if not string.endswith('/'):
        string += '/'
    return string


def yendex(json_data):
    for idx, row in enumerate(json_data, start=2):

        if row['product_link']:  # Adjust this based on your JSON structure
            url = row['product_link'].split("?")[0].replace("/list", "").replace("/spec", "").replace("/reviews", "")
            url = add_slash_to_last(url)

            rt = 0
            flag = False

            while True:
                try:
                    r = session.get(url, headers=headers)
                    s = BeautifulSoup(r.text, 'html.parser')
                    href = s.select_one('.EQlfk._38X5e')['href']
                    print(href)
                    break
                except:
                    rt += 1
                    if rt == 2:
                        data = {
                            "product_sku": row['product_sku'],
                            "adminproductname": row['adminproductname'],
                            "product_link": row['product_link'],
                            "scope": row['scope'],
                            "source_code": row['source_code'],
                            "data_score": "",
                            "data_author": "",
                            "data_published": "",
                            "data_published_parsed": "",
                            "data_content": "",
                            "url_status": "No reviews"
                        }
                        flag = True
                        dataset.append(data)
                        break

            # if flag:
            #     print("Flag", flag)
            #     continue
            
            for i in range(1, 100):
                try:
                    path = href.split('/')
                    product_id = path[2]
                    slug = 'wspe7h616'
                    sku = path[-2]
                    unique_id = path[-1].split('&')[1].split('=')[1]
                    do_waremd5 = path[-1].split('&')[2].split('=')[1]

                    json_data_request = {
                        'params': [
                            {
                                'productId': product_id,
                                'pageNum': i,
                                'withPhoto': False,
                                'restParams': {
                                    'slug': slug,
                                    'sku': sku,
                                    'uniqueId': unique_id,
                                    'do-waremd5': do_waremd5,
                                },
                            },
                        ],
                        'path': href,
                    }

                except:
                    path = href.split('/')
                    product_id = path[2]
                    slug = path[1].replace('product--', '')

                    json_data_request = {
                        'params': [
                            {
                                'productId': int(product_id),
                                'pageNum': i,
                                'withPhoto': False,
                                'restParams': {
                                    'slug': slug,
                                },
                            },
                        ],
                        'path': href,
                    }

                response = session.post(
                    'https://market.yandex.ru/api/resolve/?r=reviews/product:resolveProductReviewListState',
                    headers=headers,
                    json=json_data_request
                )

                j = response.json()

                if not j['results'][0]['data']['collections']['review']:
                    break
                
                li = {key: value.get("publicDisplayName") for key, value in j['results'][0]['data']['collections']["publicUser"].items()}

                for r_id, review in j['results'][0]['data']['collections']['review'].items():
                    data_score = review['averageGrade']
                    data_author = li.get(str(review['userId']), 'Имя скрыто')
                    data_published = review['created']
                    data_content = f"Достоинства: {review['pro']} Недостатки: {review['contra']} Комментарий:{review['comment']}"
                    
                    parsed_date = datetime.datetime.fromtimestamp(data_published / 1000)
                    formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S")

                    data = {
                        "product_sku": row['product_sku'],
                        "adminproductname": row['adminproductname'],
                        "product_link": row['product_link'],
                        "scope": row['scope'],
                        "source_code": row['source_code'],
                        "data_score": data_score,
                        "data_author": data_author,
                        "data_published": formatted_date,
                        "data_published_parsed": formatted_date,
                        "data_content": data_content,
                        "url_status": "Data extracted"
                    }

                    print(data)
                    dataset.append(data)

    df1 = pd.DataFrame(dataset)
    df1.to_excel(f'OUTPUT_new.xlsx', index=False)



yendex(load_items())