from kafka import KafkaConsumer
import redis
import json
import os

consumer = KafkaConsumer(
    'logs',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

r = redis.Redis(host='redis', port=6379, db=0)

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "logs.txt")

for message in consumer:
    log = message.value
    key = f"log:{log['timestamp']}"
    r.set(key, json.dumps(log))
    with open(log_file_path, "a") as f:
        f.write(json.dumps(log) + "\n")
    print(f"Cached and wrote log: {log}")