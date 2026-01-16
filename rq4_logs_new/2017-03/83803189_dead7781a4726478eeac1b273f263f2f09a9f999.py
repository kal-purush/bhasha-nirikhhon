import threading
import time


class Mythread(threading.Thread):

    def __init__(self, name=None,target=None):
        super().__init__(name=name, target=target)

    def myrun(self):
        self.start()
        

class Monitor():
    i = 0
    def __init__(self, phrase):
        self.stopit = False
        self.phrase = phrase
        Monitor.i += 1
        if Monitor.i == 2:
            raise RuntimeError("Can't create another monitor")
        
        t = Mythread(target=self.forever)
        t.myrun()
        print('threads running: {}'.format(threading.active_count()))

    def forever(self):
        while True:
            time.sleep(1)
            print("{}".format(self.phrase))
            if self.stopit:
                break
            
    def mystop(self):
        self.stopit = True 
        
a = Monitor('lalalala')
b = Monitor('lolololo')

a.mystop()
b.mystop()