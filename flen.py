import json
import pandas as pd

with open("cleaned.json") as f:
    data = json.load(f)
    
    df = pd.DataFrame(data)
    df.to_excel('scraped_data_item.xlsx', index=False)