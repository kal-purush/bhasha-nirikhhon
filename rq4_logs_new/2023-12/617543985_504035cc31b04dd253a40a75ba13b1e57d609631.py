#lets assume that the car is moving in porur area

#compute its speed as a demo value for the input
import random
import requests
from playsound import playsound
class CarSimulator:
    def __init__(self):
        self.accelerator_pedal = 0.0
        self.steering_angle = 0.0
        self.speed = 0.0
        self.x = 0.0  # X-coordinate on the map
        self.y = 0.0  # Y-coordinate on the map

    def touch_pedal(self, pressure):
        self.accelerator_pedal = pressure

    def turn_steering_wheel(self, angle):
        self.steering_angle = angle

    def update_speed(self):
        # Simulating the relationship between pedal pressure and speed increase
        self.speed += self.accelerator_pedal * random.uniform(20, 60)

    def update_position(self):
        # Simulating the movement of the car based on speed and steering angle
        self.x += self.speed * self.steering_angle * random.uniform(0.8, 1.2)
        self.y += self.speed * random.uniform(0.8, 1.2)

    def get_accelerometer_reading(self):
        # Simulating accelerometer readings based on speed
        return self.speed * random.uniform(0.8, 1.2)

    def get_position(self):
        return self.x, self.y


# Create an instance of the CarSimulator
car_simulator = CarSimulator()

# Simulate touching the accelerator pedal
pressure = 0.8  # Adjust the pressure value based on how much the pedal is pressed
car_simulator.touch_pedal(pressure)

# Simulate turning the steering wheel
angle = 0.1  # Adjust the angle value for steering
car_simulator.turn_steering_wheel(angle)

# Update the speed and position based on the pedal and steering input
car_simulator.update_speed()
car_simulator.update_position()

speed = int(car_simulator.speed)

# Get accelerometer reading
accelerometer_reading = car_simulator.get_accelerometer_reading()

# Get current position
position = car_simulator.get_position()

#print("Accelerometer Reading:", accelerometer_reading)
#print("Current Position:", position)
print("Current Speed:", speed)

#get the speed limit for porur area
api_key = "93v0A4i8R298ggGHjsAx488ZkH0sZc07"
latitude = 13.041671
longitude = 80.167140
base_url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
request_url = f"{base_url}?point={latitude},{longitude}&unit=KMPH&key={api_key}"
response = requests.get(request_url)
if response.status_code == 200:
    data = response.json()
    max_speed = data['flowSegmentData']['currentSpeed']
    print(f"The maximum speed limit in the Porur area is {max_speed} km/h.")
else:
    print("Error: Failed to retrieve the maximum speed limit.")
    
#compare the values and send result

if speed>max_speed:
    print('Alert! You are crossing the maximum speed limit in Porur area')


# Provide the path to the audio file
    #audio_file = '"C:\Users\Robin\Downloads\Music\mixkit-censorship-beep-1082.wav"'  # Replace with the actual path to the audio file

# Play the audio file
    #playsound(audio_file)
