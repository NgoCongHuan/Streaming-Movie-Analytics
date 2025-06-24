import os
import json
from datetime import datetime

def save_to_json(json, folder_name):

    date = datetime.now()

    parent_dir = os.getcwd()
    
    folder_path = os.path.join(parent_dir, f'backend/data/raw/{folder_name}/{date.year}/{date.month}/{date.day}')
    
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f'{folder_name}_{date.strftime('%Y_%m_%d')}.json')

    json_object = json.dumps(json, indent=4, ensure_ascii=False)

    with open(file_path, 'w', encoding='utf-8') as outfile:
        outfile.write(json_object)