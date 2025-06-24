import requests
from .utils.save_to_json import save_to_json

def get_page_count(type: int):
    """
    Sends a GET request to the Rophim API to retrieve the total number of movie pages by type.

    Args:
        type (int): Movie type. 1 = Single movie, 2 = Series movie.

    Returns:
        int: The number of pages available for the given movie type.
             Returns if any error occurs.
    """
    api_url = f'https://api.rophim.me/v1/movie/filterV2?type={type}'

    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        page_count = response.json()['result']['page_count']
        return page_count

    except requests.exceptions.RequestException as e:
        print(f'[movie] Error fetching page count for type {type}: {e}')
        return


def crawl_movie(queue):
    """
    Crawls movie data from the Rophim API based on movie type and page number.

    For each page of results, it extracts the list of movies and appends it to a master list.
    The function also puts a list of slug and _id pairs for each movie into the given queue
    for further processing. At the end, all collected movie data is saved to a JSON file.

    Args:
        queue (Queue): A thread-safe queue used to store movie slug and ID pairs.

    Returns:
        None
    """
    types = [1, 2]  # 1 = Single movie, 2 = Series movie
    list_movie = []

    for type in types:
        page_count = get_page_count(type)

        for page in range(1, page_count + 1):
            api_url = (
                f"https://api.rophim.me/v1/movie/filterV2?type={type}"
                f"&exclude_status=Upcoming&sort=release_date&page={page}"
            )

            try:
                response = requests.get(api_url, timeout=10)
                response.raise_for_status()
                movies = response.json()['result']['items']

                list_movie.append(movies)

                queue.put([
                    {'slug': m['slug'], '_id': m['_id']} for m in movies
                ])

                print(f'[movie] Successfully crawled page {page}/{page_count} for type {type}')

            except requests.exceptions.RequestException as e:
                print(f'[movie] Error crawling page {page} for type {type}: {e}')
                return

    save_to_json(list_movie, 'movie')
