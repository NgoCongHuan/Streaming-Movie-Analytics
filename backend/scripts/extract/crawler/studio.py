import re
import requests
from unidecode import unidecode
from bs4 import BeautifulSoup
from .utils.save_to_json import save_to_json

def crawl_studio(queue):
    """
    Crawls studio-related information (networks, production companies, directors) 
    for a batch of movies given in the queue.

    For each movie, it sends a request to the Rophim website, parses the HTML content, 
    and extracts relevant entities using BeautifulSoup.

    Args:
        queue (Queue): A thread-safe queue containing a list of dictionaries with 'slug' and '_id' for each movie.

    Returns:
        None
    """
    try:
        # Get a batch of movie slugs and IDs from the queue
        list_slug_id = queue.get(timeout=10)
    except Exception:
        print('[studio] Queue is empty')
        return

    list_studio = []  # Final list to store all studio info

    for slug_id in list_slug_id:
        slug = slug_id['slug']
        _id = slug_id['_id']

        studio = {"_id": _id}

        try:
            # Send GET request to the movie page
            response = requests.get(f'https://www.rophim.me/phim/{slug}.{_id}')
            soup = BeautifulSoup(response.content, 'html.parser')

            # Parse all "detail-line" sections for entity info
            for div in soup.find_all('div', class_='detail-line'):
                label = div.text.strip()

                if label.startswith('Networks'):
                    studio['networks'] = extract_entities(div)

                elif label.startswith('Sản xuất'):
                    studio['production_companies'] = extract_entities(div)

                elif label.startswith('Đạo diễn'):
                    studio['directors'] = extract_entities(div)

        except requests.exceptions.RequestException as e:
            print(f'[studio] Error fetching data for slug={slug}: {e}')

        list_studio.append(studio)

    # Save result to JSON
    save_to_json(list_studio, 'studio')


def convert_to_slug(text):
    """
    Converts a string to a URL-friendly slug using unidecode and regex.

    Args:
        text (str): The input text to convert.

    Returns:
        str: A slugified version of the text.
    """
    try:
        text = unidecode(text).lower()
        text = re.sub(r'[^a-z0-9]+', '-', text).strip('-')
        return text
    except Exception:
        print('[studio] Failed to convert to slug')
        return


def extract_entities(div):
    """
    Extracts entity information (e.g., directors, producers) from a BeautifulSoup <div> element.

    Args:
        div (bs4.element.Tag): The <div> element containing entity links.

    Returns:
        List[dict]: A list of dictionaries, each with keys: '_id', 'name', and 'slug'.
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
        return
