import aiohttp
import asyncio
from .utils.save_to_json import save_to_json


def crawl_cast(queue):
    """
    Entry point function to run the asynchronous cast crawler inside a thread.
    """
    asyncio.run(crawl_cast_async(queue))


async def crawl_cast_async(queue):
    """
    Asynchronously consumes a queue of movie IDs and fetches cast information
    from the Rophim API for each movie.

    Args:
        queue (Queue): A thread-safe queue containing a list of movie dictionaries
                       with '_id' and 'slug' keys.
    """
    list_cast = []  # Store all cast information

    while True:
        list_slug_id = queue.get()

        if list_slug_id is None:
            queue.task_done()
            break  # Exit when poison pill (None) is received

        # Build URLs for cast data
        api_urls = [
            f"https://api.rophim.me/v1/movie/casts/{slug_id['_id']}" 
            for slug_id in list_slug_id
        ]

        async with aiohttp.ClientSession() as session:
            tasks = [fetch_data(session, url) for url in api_urls]
            results = await asyncio.gather(*tasks)

        # Flatten the results (list of lists) into one list
        list_cast += [item for sublist in results for item in sublist]

        queue.task_done()

    # Save collected data
    save_to_json(list_cast, 'cast')
    print('[cast] Successfully crawled all cast')


async def fetch_data(session, url):
    """
    Sends an asynchronous GET request and extracts the 'result' field from the JSON response.

    Args:
        session (aiohttp.ClientSession): The active HTTP session.
        url (str): The API endpoint to fetch cast data.

    Returns:
        list: A list of cast members for one movie.
    """
    async with session.get(url) as response:
        await asyncio.sleep(1)  # Wait a bit to avoid spamming the API
        data = await response.json()
        return data.get('result', [])
