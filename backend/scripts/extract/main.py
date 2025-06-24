import queue
import threading

from crawler.cast_of_movie import *
from crawler.movie import *
from crawler.production_and_distribution import *

quene = queue.Queue()

if __name__ == '__main__':
    movie = threading.Thread(target=crawl_movie, args=(quene,))
    cast_of_movie = [threading.Thread(target=crawl_cast_of_movie, args=(quene,)) for _ in range(3)]
    production_and_distribution = [threading.Thread(target=crawl_production_and_distribution, args=(quene,)) for _ in range(3)]

    movie.start()
    for t in cast_of_movie + production_and_distribution:
        t.start()

    movie.join()
    for t in cast_of_movie + production_and_distribution:
        t.join()

