import aiohttp
import asyncio
import requests
from .utils.save_to_json import save_to_json


def crawl_movie(cast_queue, studio_queue):
    """
    Entry point for the movie crawling process. Runs the asynchronous
    `crawl_movie_async()` function inside a thread using `asyncio.run()`.

    Args:
        cast_queue (Queue): Queue to store list of movie IDs for cast scraping.
        studio_queue (Queue): Queue to store list of movie IDs for studio scraping.
    """
    asyncio.run(crawl_movie_async(cast_queue, studio_queue))


async def crawl_movie_async(cast_queue, studio_queue):
    """
    Asynchronously crawls movie data from the Rophim API across different types and pages.
    All movie data is collected, flattened, saved, and dispatched to other worker queues.

    Steps:
    - Construct URLs for each movie type and page.
    - Perform async HTTP requests to fetch movie data.
    - Flatten the results and extract needed movie info.
    - Save the movie data to a JSON file.
    - Push movie IDs (slug + _id) to `cast_queue` and `studio_queue`.
    - Add `None` to both queues as poison pills to signal completion.

    Args:
        cast_queue (Queue): Queue for cast data processing.
        studio_queue (Queue): Queue for studio data processing.
    """
    api_urls = [
        f"https://api.rophim.me/v1/movie/filterV2?type={type}"
        f"&exclude_status=Upcoming&sort=release_date&page={page}"
        for type in [1, 2]  # type=1: Single movie, type=2: Series
        for page in range(1, get_page_count(type) + 1)
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, url) for url in api_urls]
        results = await asyncio.gather(*tasks)

    # Flatten all results into one movie list
    list_movie = [item for sublist in results for item in sublist]

    # Save the complete movie list to a JSON file
    save_to_json(list_movie, 'movie')
    print('[movie] Successfully crawled all movies')

    # Extract movie IDs for downstream tasks
    list_slug_id = [
        {'slug': m['slug'], '_id': m['_id']} 
        for m in list_movie
    ]

    # Send movie IDs to cast and studio queues
    cast_queue.put(list_slug_id)
    studio_queue.put(list_slug_id)

    # Push poison pills to indicate queue completion
    cast_queue.put(None)
    studio_queue.put(None)


def get_page_count(type: int):
    """
    Fetches the number of pages available for a given movie type using a synchronous API call.

    Args:
        type (int): Movie type. 1 = Single movie, 2 = Series movie.

    Returns:
        int: Number of pages to crawl for the specified type.
             Returns None if the request fails.
    """
    api_url = f'https://api.rophim.me/v1/movie/filterV2?type={type}'

    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        return response.json()['result']['page_count']

    except requests.exceptions.RequestException as e:
        print(f'[movie] Error fetching page count for type {type}: {e}')
        return


async def fetch_data(session, url):
    """
    Sends an asynchronous GET request using aiohttp and returns the movie items from the response.

    Args:
        session (aiohttp.ClientSession): The active session for performing HTTP requests.
        url (str): The API URL to request movie data from.

    Returns:
        list: List of movie dictionaries from the API response.
    """
    async with session.get(url) as response:
        await asyncio.sleep(1)  # Prevent spamming the server
        data = await response.json()
        return data.get('result', {}).get('items', [])
