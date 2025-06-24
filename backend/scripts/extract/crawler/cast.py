import requests
from .utils.save_to_json import save_to_json

def crawl_cast(queue):
    """
    Crawls cast information for a batch of movies from the Rophim API.

    Retrieves a list of movie IDs from a queue, then fetches the cast data
    for each movie by sending a request to the API. The results are saved to a JSON file.

    Args:
        queue (Queue): A thread-safe queue that contains a list of dictionaries 
                       with '_id' (movie ID) and 'slug' (not used here, but available).

    Returns:
        None
    """
    try:
        # Get a batch of movie IDs from the queue
        list_slug_id = queue.get(timeout=10)
    except Exception:
        print('[cast] Queue is empty')
        return

    list_cast = []  # Store cast data for all movies

    for slug_id in list_slug_id:
        _id = slug_id['_id']

        try:
            # Send GET request to fetch cast information for the movie
            response = requests.get(f'https://api.rophim.me/v1/movie/casts/{_id}', timeout=10)
            response.raise_for_status()
            cast = response.json()['result']

            list_cast.append(cast)

            print(f'[cast] Successfully crawled cast for movie ID: {_id}')

        except requests.exceptions.RequestException as e:
            print(f'[cast] Error fetching cast for movie ID {_id}: {e}')

    # Save the collected cast data to a JSON file
    save_to_json(list_cast, 'cast')
