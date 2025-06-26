import re
import requests
from unidecode import unidecode
from bs4 import BeautifulSoup
from .utils.save_to_json import save_to_json


def crawl_studio(queue):
    """
    Crawls studio-related information (networks, production companies, directors) 
    for a batch of movies by scraping the Rophim movie page.

    Args:
        queue (Queue): A thread-safe queue containing dictionaries with 'slug' and '_id' of movies.
    """
    list_studio = []

    while True:
        list_slug_id = queue.get()

        if list_slug_id is None:
            queue.task_done()
            break  # Stop when poison pill is received

        for slug_id in list_slug_id:
            slug = slug_id['slug']
            _id = slug_id['_id']
            studio = {"_id": _id}

            try:
                # Send HTTP GET request to the movie page
                response = requests.get(f'https://www.rophim.me/phim/{slug}.{_id}')
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find all detail lines and extract relevant entities
                for div in soup.find_all('div', class_='detail-line'):
                    label = div.text.strip()

                    if label.startswith('Networks'):
                        studio['networks'] = extract_entities(div)

                    elif label.startswith('Sản xuất'):
                        studio['production_companies'] = extract_entities(div)

                    elif label.startswith('Đạo diễn'):
                        studio['directors'] = extract_entities(div)

                list_studio.append(studio)
                print(f'[studio] Successfully crawled studio for movie ID: {_id}')

            except requests.exceptions.RequestException as e:
                print(f'[studio] Error fetching data for slug={slug}: {e}')

        queue.task_done()

    save_to_json(list_studio, 'studio')
    print('[studio] Successfully crawled all studios')


def extract_entities(div):
    """
    Extracts entities (e.g., directors, producers) from a BeautifulSoup <div> element.

    Args:
        div (bs4.element.Tag): The <div> containing span and anchor tags.

    Returns:
        List[dict]: List of dictionaries with '_id', 'name', and 'slug' for each entity.
    """
    try:
        entities = [
            {
                '_id': span.find('a').attrs['href'].split('.')[-1],
                'name': span.text.strip(),
                'slug': convert_to_slug(span.text)
            }
            for span in div.find_all('span')
        ]
        return entities
    except Exception:
        print('[studio] Failed to extract entities')
        return []


def convert_to_slug(text):
    """
    Converts a text string into a URL-friendly slug using unidecode and regex.

    Args:
        text (str): The original string.

    Returns:
        str: Slugified version of the string.
    """
    try:
        text = unidecode(text).lower()
        text = re.sub(r'[^a-z0-9]+', '-', text).strip('-')
        return text
    except Exception:
        print('[studio] Failed to convert to slug')
        return ""
