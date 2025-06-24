import requests
from .utils.save_to_json import save_to_json

def crawl_cast_of_movie(quene):
    try:
        list_slug_id = quene.get(timeout=10)
    except:
        print(f'[cast_of_movie] Quene is empty')
        
    list_cast_of_movie = []

    for slug_id in list_slug_id:
        
        _id = slug_id['_id']
        
        response = requests.get(f'https://api.rophim.me/v1/movie/casts/{_id}')
        cast_of_movie = response.json()['result']
        
        list_cast_of_movie.append(cast_of_movie)
        
        print(f'[cast_of_movie] Succesfull at movie ID: {_id}')
    
    save_to_json(list_cast_of_movie, 'cast_of_movie')
        
    