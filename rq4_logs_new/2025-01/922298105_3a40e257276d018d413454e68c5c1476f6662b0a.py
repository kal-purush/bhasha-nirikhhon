from flask import Flask
import redis

app = Flask(__name__)
cache = redis.StrictRedis(host='redis', port=6379, decode_responses=True)

@app.route('/')
def hello():
    count = cache.incr('hits')
    return f"Hello from Docker Compose! This page has been visited {count} times."

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)