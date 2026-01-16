class Observable(object):
    def __init__(self):
        self.observers = []
        print(self, " Observable init: ")

    def register(self, observer):
        if not observer in self.observers:
            self.observers.append(observer)
            print("all observers: ", self.observers)

    def unregister(self, observer):
        if observer in self.observers:
            self.observers.remove(observer)

    def unregister_all(self):
        if self.observers:
            del self.observers[:]

    def notify(self, data):
        print(self,  "notified ", self.observers)
        for observer in self.observers:
            observer.on_received_data(data)

