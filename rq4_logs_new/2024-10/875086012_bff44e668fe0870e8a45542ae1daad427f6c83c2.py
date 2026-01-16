import atexit
from flask import Flask, jsonify
import bosdyn.client.util
from bosdyn.client import ResponseError, RpcError, create_standard_sdk
from bosdyn.client.robot_command import RobotCommandBuilder, RobotCommandClient, blocking_command, blocking_stand, blocking_sit
from wasd import WasdInterface  # Assuming the original file is named 'wasd.py'

app = Flask(__name__)

HOSTNAME = "192.168.80.3"

# Global variables to hold robot and interface objects
robot = None
wasd_interface = None

def initialize_robot():
    global robot, wasd_interface
    print("initializing robot...")
    # Create robot object.
    sdk = create_standard_sdk('WASDClient')
    robot = sdk.create_robot(HOSTNAME)
    try:
        bosdyn.client.util.authenticate(robot)
        robot.start_time_sync()
    except RpcError as err:
        print("Failed to communicate with robot: %s", err)
        return False

    wasd_interface = WasdInterface(robot)
    try:
        print("starting control interface...")
        wasd_interface.start()
    except (ResponseError, RpcError) as err:
        print("Failed to initialize robot communication: %s", err)
        return False

    try:
        print("estop state:")
        print(wasd_interface._estop_str())
        print("toggling estop")
        wasd_interface._toggle_estop()
        print("estop state:")
        print(wasd_interface._estop_str())
        robot.power_on(timeout_sec=20)
        if robot.is_powered_on():
            print("Powered On")
            # If everything went smooth, Spot face lights should turn green
        else:
            # In case of some problems, e.g. somebody stole control over robot
            print("Power on failed")
        # idk if this works yet
        robot.time_sync.wait_for_sync()
        command_client = robot.ensure_client(RobotCommandClient.default_service_name)
        print("standing...")
        blocking_stand(command_client, timeout_sec=10)
        print("stand command complete")
        
    except (ResponseError, RpcError) as err:
        print("Failed to toggle estop or power: %s", err)
        return False

    # Register shutdown method to be called upon Flask app shutdown
    atexit.register(wasd_interface.shutdown)

# push context manually to app
with app.app_context():
    initialize_robot()

@app.route('/move/forward', methods=['GET'])
def move_forward():
    wasd_interface._move_forward()
    return jsonify(status="Moving forward")

@app.route('/move/backward', methods=['GET'])
def move_backward():
    wasd_interface._move_backward()
    return jsonify(status="Moving backward")

@app.route('/move/left', methods=['GET'])
def move_left():
    wasd_interface._strafe_left()
    return jsonify(status="Moving left")

@app.route('/move/right', methods=['GET'])
def move_right():
    wasd_interface._strafe_right()
    return jsonify(status="Moving right")

@app.route('/turn/left', methods=['GET'])
def turn_left():
    wasd_interface._turn_left()
    return jsonify(status="Turning left")

@app.route('/turn/right', methods=['GET'])
def turn_right():
    wasd_interface._turn_right()
    return jsonify(status="Turning right")

@app.route('/sit', methods=['GET'])
def sit():
    wasd_interface._sit()
    return jsonify(status="Sitting")

@app.route('/stand', methods=['GET'])
def stand():
    wasd_interface._stand()
    return jsonify(status="Standing")

@app.route('/battery-change', methods=['GET'])
def battery_change():
    wasd_interface._battery_change_pose()
    return jsonify(status="Battery change position")

@app.route('/battery', methods=['GET'])
def battery():
    return jsonify(wasd_interface._battery_str())

@app.route('/self-right', methods=['GET'])
def self_righ():
    wasd_interface._self_right()
    return jsonify(status="Self correcting...")

@app.route('/shutdown', methods=['GET'])
def shutdown():
    wasd_interface.shutdown()
    return jsonify(status="Shutdown complete")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)