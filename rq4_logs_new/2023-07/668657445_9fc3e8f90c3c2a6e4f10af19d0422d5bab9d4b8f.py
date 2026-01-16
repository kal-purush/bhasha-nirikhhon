from dronekit import connect, VehicleMode, LocationGlobalRelative
import time

# Main Function
def moveWayPoint():

    way_points = {
        0: {'lat': 35.878934, 'lon': 140.339687, 'alt': 10 },
        1: {'lat': 35.878601, 'lon': 140.339197, 'alt': 10 },
        2: {'lat': 35.878873, 'lon': 140.3388642, 'alt': 10},
        3: {'lat': 35.879138, 'lon': 140.339382, 'alt': 10 },
    }

    for i in range(4):
        _way_points = way_points[i]
        aLocation = LocationGlobalRelative(_way_points['lat'],  _way_points['lon'], _way_points['alt'])
        vehicle.simple_goto(aLocation)
        time.sleep(15)
    print("Mission Conmplete")

# Connecting...
vehicle = connect('tcp:127.0.0.1:5762', wait_ready=True, timeout=60)

# Set HomeLocation
while not vehicle.home_location:
    cmds = vehicle.commands
    cmds.download()
    cmds.wait_ready()
    if not vehicle.home_location:
        print("Waiting for home location ...")

print("Home location: %s " % vehicle.home_location)

# Ready to TakeOff
try:
    vehicle.wait_for_armable()
    print("Ready to Arm")
    vehicle.wait_for_mode("GUIDED")
    print("Mode Change Guided")
    vehicle.groundspeed = 4.0
    vehicle.arm()
    print("Arm")
    time.sleep(1)
    print("Taking Off")
    vehicle.wait_simple_takeoff(10, timeout=20)
    
except TimeoutError:
    vehicle.mode = VehicleMode("RTL")
    print("TimeOUT")

# Run Mission
try:
    moveWayPoint()
    vehicle.mode = VehicleMode("RTL")
    print("RTL")

except:
    print("TimeOUT")
    vehicle.mode = VehicleMode("RTL")