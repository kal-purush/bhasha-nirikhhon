import cv2
import time
import pika
import json

# Set the time interval in seconds
interval = 20  # 5 minutes = 300 seconds

# Initialize the webcam
cap = cv2.VideoCapture(0)  # 0 represents the default camera (you can change it to a specific camera index if you have multiple cameras)

# Check if the webcam is opened successfully
if not cap.isOpened():
    print("Error: Could not open the webcam.")
    exit()

# RabbitMQ connection parameters
rabbitmq_host = 'localhost'
rabbitmq_port = 5672
rabbitmq_username = 'guest'
rabbitmq_password = 'guest'
queue_name = 'review-picture-queue'

# Initialize RabbitMQ connection and channel with authentication
credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials)
)
channel = connection.channel()

# Declare a queue for sending messages
channel.queue_declare(queue=queue_name)

while True:
    # Capture a frame from the webcam
    ret, frame = cap.read()

    if not ret:
        print("Error: Could not read frame from the webcam.")
        break

    # Save the captured frame as an image
    timestamp = time.strftime("%Y%m%d%H%M%S")
    filename = f"image_{timestamp}.jpg"
    cv2.imwrite(filename, frame)

    print(f"Image captured and saved as {filename}")

    # Prepare a JSON message
    message = {
        'fileName': filename,
        'timestamp': timestamp,
    }
    message_json = json.dumps(message)

    # Send the JSON message to RabbitMQ
    channel.basic_publish(exchange='', routing_key=queue_name, body=message_json)

    print(f"Message sent to RabbitMQ: {message_json}")

    # Wait for the specified interval
    time.sleep(interval)

# Release the webcam, close RabbitMQ connection, and close any open windows
cap.release()
connection.close()
cv2.destroyAllWindows()