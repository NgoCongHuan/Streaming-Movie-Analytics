import os
import json
from datetime import datetime

def save_to_json(data, folder_name):
    date = datetime.now()

    current_dir = os.path.dirname(os.path.abspath(__file__))  # .../crawler/utils
    project_root = os.path.abspath(os.path.join(current_dir, "../../../../"))

    folder_path = os.path.join(
        project_root,
        f'data/raw/{folder_name}/{date.year}/{date.month}/{date.day}'
    )

    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(
        folder_path,
        f'{folder_name}_{date.strftime("%Y_%m_%d")}.json'
    )

    # Debug
    print("🟩 Saving to:", file_path)
    print("🟨 Number of items:", len(data))
    print("📂 Folder exists:", os.path.exists(folder_path))

    try:
        with open(file_path, 'w', encoding='utf-8') as outfile:
            for item in data:
                json_line = json.dumps(item, ensure_ascii=False)
                outfile.write(json_line + '\n')
        print(f"✅ Saved file to {file_path}")
    except Exception as e:
        print(f"❌ Error saving file: {e}")
