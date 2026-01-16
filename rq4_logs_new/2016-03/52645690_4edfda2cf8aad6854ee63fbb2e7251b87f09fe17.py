from celery import Celery

app = Celery('tasks', backend='redis://localhost', broker='amqp://guest@localhost//')

@app.task
def add(x, y):
    return x + y

@app.task
def mul(x, y):
    return x * y

@app.task
def xsum(numbers):
    return sum(numbers)