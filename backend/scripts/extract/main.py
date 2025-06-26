import threading
import queue

from crawler.movie import crawl_movie
from crawler.cast import crawl_cast
from crawler.studio import crawl_studio

if __name__ == '__main__':
    
    cast_queue = queue.Queue()
    studio_queue = queue.Queue()

    movie_thread = threading.Thread(target=crawl_movie, args=(cast_queue, studio_queue))
    cast_thread = threading.Thread(target=crawl_cast, args=(cast_queue,))
    studio_thread = threading.Thread(target=crawl_studio, args=(studio_queue,))

    movie_thread.start()
    cast_thread.start()
    studio_thread.start()

    movie_thread.join()
    cast_thread.join()
    studio_thread.join()
