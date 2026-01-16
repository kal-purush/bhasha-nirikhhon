import time
import paho.mqtt.client as mqtt

from datetime import datetime

# MQTT broker settings
broker_address = "broker"  # Change this to your broker's address
port = 1883  # Default MQTT port
topic = "test/topic"  # Change this to your desired topic

# Create an MQTT client instance
client = mqtt.Client()

# Connect to the broker
client.connect(broker_address, port=port)

try:
    while True:
        # Message to publish
        message = "New detection: " + str(datetime.now())

        # Publish the message
        client.publish(topic, message)

        # Print a confirmation message
        print(f"Published: {message}")

        # Wait for 5 seconds before sending the next message
        time.sleep(5)

except KeyboardInterrupt:
    # Handle Ctrl+C gracefully
    print("Interrupted by user")

finally:
    # Disconnect from the broker when done
    client.disconnect()