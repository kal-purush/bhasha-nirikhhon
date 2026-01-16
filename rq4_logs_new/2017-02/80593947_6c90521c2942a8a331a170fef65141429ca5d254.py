__author__ = 'jidziak'

import threading
total_distance = 0

lock = threading.Lock()
class runner(threading.Thread):
    def __init__(self, nr):
        self.numer = nr
        threading.Thread.__init__(self)
    def run(self):
        global lock
        global total_distance
        dystans = 421095
        while dystans > 0:
            dystans = dystans - 1
            lock.acquire()
            total_distance = total_distance + 1
            lock.release()
        if dystans % 10000 == 0:
            print ("Zawodnik nr {0}".format(self.numer))
        print ("Zawodnik {0} na mecie".format(self.numer))

r1 = runner(1)
r2 = runner(2)
r1.start()
r2.start()
r1.join()
r2.join()
print ("koniec wyścigu, dystans {0}".format(total_distance))

import multiprocessing
pr = multiprocessing.Process(target=callable, args=sequence)
pr.start()
pr.join()

q = multiprocessing.Queue()
q.put(wartosc)
q.get()