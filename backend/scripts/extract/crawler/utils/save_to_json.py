import os
import json
from datetime import datetime

def save_to_json(data, folder_name):
    """
    Save a list of JSON objects to a file in NDJSON format (newline-delimited JSON).

    Args:
        data (list): A list of dictionaries, where each dictionary represents a JSON object.
        folder_name (str): The name of the subfolder to store the data 
                           (e.g., 'movie', 'cast', 'studio').
    """
    date = datetime.now()

    parent_dir = os.getcwd()
    folder_path = os.path.join(
        parent_dir,
        f'backend/data/raw/{folder_name}/{date.year}/{date.month}/{date.day}'
    )
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(
        folder_path,
        f'{folder_name}_{date.strftime("%Y_%m_%d")}.json'
    )

    # Write each dictionary as a JSON object on a separate line (NDJSON format)
    with open(file_path, 'w', encoding='utf-8') as outfile:
        for item in data:
            json_line = json.dumps(item, ensure_ascii=False)
            outfile.write(json_line + '\n')
