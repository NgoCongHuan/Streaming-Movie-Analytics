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
    list_cast = []  # Store cast data for all movies

    while True:
        
        list_slug_id = queue.get()
        if list_slug_id is None:
            queue.task_done()
            break
        
        for slug_id in list_slug_id:
            _id = slug_id['_id']

            try:
                # Send GET request to fetch cast information for the movie
                response = requests.get(f'https://api.rophim.me/v1/movie/casts/{_id}', timeout=10)
                response.raise_for_status()
                cast = response.json()['result']

                list_cast += cast

                print(f'[cast] Successfully crawled cast for movie ID: {_id}')

            except requests.exceptions.RequestException as e:
                print(f'[cast] Error fetching cast for movie ID {_id}: {e}')
        
        queue.task_done()

    # Save the collected cast data to a JSON file
    save_to_json(list_cast, 'cast')
