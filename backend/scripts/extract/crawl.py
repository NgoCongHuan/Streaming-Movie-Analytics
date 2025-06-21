import threading
import queue
import requests
import json
from unidecode import unidecode
import re
import os
from bs4 import BeautifulSoup

id_queue = queue.Queue()

def get_page_count(type: int):
    """
    """
    api_url = f'https://api.rophim.me/v1/movie/filterV2?type={type}'
    response = requests.get(api_url)
    page_count = response.json()['result']['page_count']
    return page_count

def crawl_movies():
    """
    """
    # Movie Type: 1 is Single movie, 2 is Series movie
    types = [1, 2]
    
    # Total movies
    list_movies = []

    for type in types:

        page_count = get_page_count(type)

        for page in range(1,5):
            
            api_url = f"https://api.rophim.me/v1/movie/filterV2?type={type}&exclude_status=Upcoming&sort=release_date&page={page}"
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            movies = response.json()['result']['items']

            list_movies.append(movies)

            print(f'Succesfull at page {page}')

            list_slug_id = [
                {
                'slug':movie['slug'],
                '_id':movie['_id']
                } 
                for movie in movies]

            id_queue.put(list_slug_id)
    
    json_object = json.dumps(list_movies, indent=4, ensure_ascii=False)

    # Lấy đường dẫn tuyệt đối tới thư mục hiện tại của crawl.py
    current_dir = os.path.dirname(__file__)

    # Đi đến folder raw
    raw_dir = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'raw'))

    # Tạo folder raw nếu chưa tồn tại
    os.makedirs(raw_dir, exist_ok=True)

    # Tên file bạn muốn lưu
    filename = "movies.json"

    # Đường dẫn đầy đủ đến file
    file_path = os.path.join(raw_dir, filename)

    with open(file_path, 'w', encoding='utf-8') as outfile:
        outfile.write(json_object)

def crawl_cast_movie():
    list_slug_id = id_queue.get(timeout=10)
    print(list_slug_id)
    list_cast = []
    for id in list_slug_id:
        response = requests.get(f'https://api.rophim.me/v1/movie/casts/{id['_id']}')
        response.raise_for_status()
        casts = response.json()['result']

        list_cast.append(casts)

        print(f'Succesfull at movie {id}')
    
    json_object = json.dumps(list_cast, indent=4, ensure_ascii=False)

    # Lấy đường dẫn tuyệt đối tới thư mục hiện tại của crawl.py
    current_dir = os.path.dirname(__file__)

    # Đi đến folder raw
    raw_dir = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'raw'))

    # Tạo folder raw nếu chưa tồn tại
    os.makedirs(raw_dir, exist_ok=True)

    # Tên file bạn muốn lưu
    filename = "casts.json"

    # Đường dẫn đầy đủ đến file
    file_path = os.path.join(raw_dir, filename)

    with open(file_path, 'w', encoding="utf-8") as outfile:
        outfile.write(json_object)

def convert_to_slug(text):
    text = unidecode(text)
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    return text

def generate_dictionary(text):
    dic = {
        '_id': text.find('a').attrs['href'].split('.')[-1],
        'name': text.text,
        'slug': convert_to_slug(text.text)
    }
    return dic

def crawl_information():
    list_information = []
    list_slug_id = id_queue.get(timeout=10)

    for slug_id in list_slug_id:
        response = requests.get(f'https://www.rophim.me/phim/{slug_id['slug']}.{slug_id['_id']}')
        soup = BeautifulSoup(response.content, 'html.parser')

        dic = {"_id": slug_id['_id']}
        for line in soup.find_all('div', class_='detail-line'):
            

            if line.text.startswith('Networks'):
                networks = [generate_dictionary(n) for n in line.find_all('span')]
                dic.update({'networks': networks})
                print(f'networks: {networks}')

            if line.text.startswith('Sản xuất'):
                production_companies = [generate_dictionary(pc) for pc in line.find_all('span')]
                dic.update({'production_companies': production_companies})
                print(f'production_companies: {production_companies}')
            
            if line.text.startswith('Đạo diễn'):
                directors = [generate_dictionary(d) for d in line.find_all('span')]
                dic.update({'directors': directors})
                print(f'directors: {directors}')

        list_information.append(dic)
        
    json_object = json.dumps(list_information, indent=4, ensure_ascii=False)

    # Lấy đường dẫn tuyệt đối tới thư mục hiện tại của crawl.py
    current_dir = os.path.dirname(__file__)

    # Đi đến folder raw
    raw_dir = os.path.abspath(os.path.join(current_dir, '..', '..', 'data', 'raw'))

    # Tạo folder raw nếu chưa tồn tại
    os.makedirs(raw_dir, exist_ok=True)

    # Tên file bạn muốn lưu
    filename = "list_information.json"

    # Đường dẫn đầy đủ đến file
    file_path = os.path.join(raw_dir, filename)

    with open(file_path, 'w', encoding="utf-8") as outfile:
        outfile.write(json_object)

producer_thread = threading.Thread(target=crawl_movies)
consumer_threads = [threading.Thread(target=crawl_cast_movie) for _ in range(5)]
consumer_threads_2 = [threading.Thread(target=crawl_information) for _ in range(5)]

# Chạy
producer_thread.start()
for t in consumer_threads:
    t.start()
for t in consumer_threads_2:
    t.start()

producer_thread.join()
for t in consumer_threads:
    t.join()
for t in consumer_threads_2:
    t.join()
