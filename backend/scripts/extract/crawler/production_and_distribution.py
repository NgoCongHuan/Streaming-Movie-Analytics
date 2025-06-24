import re
import requests
from unidecode import unidecode
from bs4 import BeautifulSoup
from .utils.save_to_json import save_to_json

def crawl_production_and_distribution(quene):
    
    try:
        list_slug_id = quene.get(timeout=10)
    except:
        print(f'[cast_of_movie] Quene is empty')

    list_production_and_distribution = []

    for slug_id in list_slug_id:
        
        slug = slug_id['slug']
        _id = slug_id['_id']

        production_and_distribution = {"_id": _id}
        
        response = requests.get(f'https://www.rophim.me/phim/{slug}.{_id}')
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for div in soup.find_all('div', class_='detail-line'):
            
            label = div.text.strip()

            if label.startswith('Networks'):
                production_and_distribution['networks'] = extract_entities(div)

            elif label.startswith('Sản xuất'):
                production_and_distribution['production_companies'] = extract_entities(div)
            
            elif label.startswith('Đạo diễn'):
                production_and_distribution['directors'] = extract_entities(div)

        list_production_and_distribution.append(production_and_distribution)
    
    save_to_json(list_production_and_distribution, 'production_and_distribution')

def convert_to_slug(text):
    text = unidecode(text).lower()
    text = re.sub(r'[^a-z0-9]+', '-', text).strip('-')
    return text

def extract_entities(div):
    return [
        {
        '_id': a.find('a').attrs['href'].split('.')[-1],
        'name': a.text,
        'slug': convert_to_slug(a.text)
        }
        for a in div.find_all('span')  
    ]