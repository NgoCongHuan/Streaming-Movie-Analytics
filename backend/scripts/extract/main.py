import queue
import threading

from crawler.cast import crawl_cast
from crawler.movie import crawl_movie
from crawler.studio import crawl_studio

quene = queue.Queue()

if __name__ == '__main__':
    movie = threading.Thread(target=crawl_movie, args=(quene,))
    cast = [threading.Thread(target=crawl_cast, args=(quene,)) for _ in range(3)]
    studio = [threading.Thread(target=crawl_studio, args=(quene,)) for _ in range(3)]

    movie.start()
    for t in cast + studio:
        t.start()

    movie.join()
    for t in cast + studio:
        t.join()

