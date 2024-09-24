import json


with open('scraped_data_api.json', 'r') as json_file:
    data = json.load(json_file)
    
    
    for review in data:
        if len(review['description']) >= 5:
            review['url_status'] = "Data Extracted"

    else:
        review['url_status'] = "No Reviews"
    
    with open('cleaned.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)