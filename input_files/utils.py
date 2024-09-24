import pandas as pd
import json
import os
from pathlib import Path

current_directory = Path(__file__).parent


def excel_to_json(excel_file_path):
    df = pd.read_excel(excel_file_path)
    json_data = df.to_json(orient='records')
    parsed_json = json.loads(json_data)
    return parsed_json


def list_xlsx_files():
    xlsx_files = list(current_directory.glob('*.xlsx'))
    return [file.name for file in xlsx_files]


def load_input_files():
    files = list_xlsx_files()
    for file in files:
        yield excel_to_json(current_directory / file)


def load_items():
    all_items = []
    for json_data in load_input_files():
        for item in json_data:
            if 'product_link' in item:
                all_items.append(item)
    return all_items
