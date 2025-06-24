import requests
from .utils.save_to_json import save_to_json

def get_page_count(type: int):
    """
    """
    try:
        api_url = f'https://api.rophim.me/v1/movie/filterV2?type={type}'
        response = requests.get(api_url)
        page_count = response.json()['result']['page_count']
        return page_count
    except Exception as e:
        print(f'Error: {e}')
    

def crawl_movie(quene):
    """
    """
    # Movie Type: 1 is Single movie, 2 is Series movie
    types = [1, 2]
    
    # Total movies
    list_movie = []
    
    try:
        for type in types:

            page_count = get_page_count(type)

            for page in range(1, 5):
                
                api_url = f"https://api.rophim.me/v1/movie/filterV2?type={type}&exclude_status=Upcoming&sort=release_date&page={page}"
                response = requests.get(api_url, timeout=10)
                movies = response.json()['result']['items']

                list_movie.append(movies)

                

                list_slug_id = [
                    {'slug':m['slug'], '_id':m['_id']} 
                    for m in movies
                ]

                quene.put(list_slug_id)

                print(f'[list_movie] Succesfull crawled page {page} for type {type}')
        
        save_to_json(list_movie, 'list_movie')

    except Exception as e:
        print(f'Error: {e}')