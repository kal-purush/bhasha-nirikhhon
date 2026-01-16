import threading
from queue import Queue
from spider import Spider
from domain import *
from general import *

WEBSITE_NAME = "micown"
HOMEPAGE = "http://micown.com/"
DOMAIN_NAME = get_domain(HOMEPAGE)
QUEUE_FILE = WEBSITE_NAME + "/queue.txt"
CRAWLED_FILE = WEBSITE_NAME + "/crawled.txt"
NUMBER_OF_THREADS = 8
queue = Queue()
Spider(WEBSITE_NAME, HOMEPAGE, DOMAIN_NAME)