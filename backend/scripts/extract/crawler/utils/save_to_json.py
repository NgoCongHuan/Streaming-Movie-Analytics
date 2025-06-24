import os
import json
from datetime import datetime

def save_to_json(dictionary, file_name):
    date = datetime.now()

    parent_dir = os.getcwd()
    # Tạo đường dẫn đến thư mục (chỉ tới cấp chứa file)
    folder_path = os.path.join(parent_dir, f'backend/data/raw/{file_name}/{date.year}/{date.month}/{date.day}')
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(folder_path, exist_ok=True)

    # Tạo tên file
    file_path = os.path.join(folder_path, f'{file_name}_{date.day}_{date.month}_{date.year}.json')

    # Chuyển dict sang JSON và ghi vào file
    json_object = json.dumps(dictionary, indent=4, ensure_ascii=False)

    with open(file_path, 'w', encoding='utf-8') as outfile:
        outfile.write(json_object)
