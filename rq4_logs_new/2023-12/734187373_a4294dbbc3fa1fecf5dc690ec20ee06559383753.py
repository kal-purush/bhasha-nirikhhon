from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import json

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Sample data
data = [
    {"track_id": "5SuOikwiRyPMVoIQDJUgSV", "artists": "Gen Hoshino", "album_name": "Comedy", "track_name": "Comedy", "popularity": 73, "duration_ms": 230666, "explicit": False, "danceability": 0.676, "energy": 0.461, "key": 1, "loudness": -6.746, "mode": 0, "speechiness": 0.143, "acousticness": 0.0322, "instrumentalness": 1.01E-06, "liveness": 0.358, "valence": 0.715, "tempo": 87.917, "time_signature": 4, "track_genre": "acoustic"},
    # Add other data here...
]

# Send data to Kafka
for item in data:
    producer.send('spotify', value=item)

# Create a Kafka consumer
consumer = KafkaConsumer('spotify',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['spotifydb']
collection = db['Tracks']
print(consumer)
# Fetch data from Kafka and store it in MongoDB
for message in consumer:
    print(message)
    message = message.value
    collection.insert_one(message)