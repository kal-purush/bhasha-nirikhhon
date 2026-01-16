class Logger:

    def __init__(self):

        self.queue = []
        self.curr_time = 0
        self.items = set()

    def clear_logger(self): # We clear the logger with all
        topop = 0
        for q in self.queue:
            if q[1]+10<=self.curr_time:
                topop += 1
                self.items.remove(q[0]) # Removing items from hash
        
        while topop>0:
            self.queue.pop(0)
            topop -= 1

    def should_print(self, timestamp, message):
        self.curr_time = timestamp
        self.clear_logger()

        if message in self.items: # This means we can't print
            return False 
        
        self.queue.append((message, timestamp))
        self.items.add(message)

        return True 

logger = Logger()
logger.should_print(1, "foo")
logger.should_print(2, "bar")
logger.should_print(3, "foo")
logger.should_print(8, "bar")
logger.should_print(10, "foo")
logger.should_print(11, "foo")
logger.should_print(11, "bar")
logger.should_print(12, "bar")
logger.should_print(12, "bar")