class Queue:
    def __init__(self):
        self.queue = []
        self.size = 0

    def enqueue(self, data):
        self.queue.append(data)
        self.size += 1

    def dequeue(self):
        
        if self.size > 0:
            self.size -= 1
            n = self.queue[0]
            self.queue = self.queue[1:]
        
            return n
            
        return None

    def peek(self):
        if self.size > 0:
            return self.queue[0]
            
        return None


    def is_Empty(self):
        return self.size == 0


    def get_size(self):
        return self.size


    def __str__(self):
        return ", ".join(map(str, self.queue))
    

q = Queue()
q.enqueue(1)
q.enqueue(2)
q.enqueue(3)

print(q.dequeue())
print(q.peek())
print(q.is_Empty())
print(q.get_size())
print(q)