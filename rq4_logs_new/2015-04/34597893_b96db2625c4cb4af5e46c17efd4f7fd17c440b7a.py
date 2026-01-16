from Tkinter import *
import serial
import math
import fileinput
import select
import sys
import time
import socket

PIXELS_PER_CM = 4

window = Tk()
HEIGHT = 600
WIDTH = 600

global autoOn
autoOn = False  # default

method = ""  # tcp or usb
tcpBuffer = ""

# Sweep between an startAngle and endAngle, adding up all of the points that corrospond to an encountered object
# Do not include any value >= dropThreshold
def getMarksBetweenAngles(startAngle, endAngle, dropThreshhold, data):
    result = 0;
    for angle in range(startAngle, endAngle):
        if (data[angle % 360] < dropThreshhold):
            result += data[(angle % 360) / dropThreshhold * 5]
    return result;


def bestAngleToTurn(dropThreshold, data):
    angleData = []
    for angle in [0, 45, 90, 135, 180, 125, 270, 315]:
        startAngle = angle - (45 / 2)
        endAngle = angle + (45 / 2)
        if (endAngle < 0):
            endAngle = 360 + endAngle
        print "Start: ", startAngle, " | End: ", endAngle
        totalMarks = getMarksBetweenAngles(startAngle, endAngle, dropThreshold, data)
        angleData.append((angle, totalMarks))
    print angleData
    tupleData = (sorted(angleData, key=lambda (angle, totalMarks): totalMarks)[0])
    if (tupleData[1] == 0):
        distance = 100
    elif (tupleData[1] < 10):
        distance = 70
    else:
        distance = 40
    return (tupleData[0], distance)


def bestAngleToTurn2(dropThreshold, data):
    angleList = []
    for i in range(0, 4):
        for dir in [1, -1]:
            startAngle = 45 * (i * dir)
            endAngle = startAngle + (45 * dir)
            startAngle = (startAngle) % 360;  # Readjust
            endAngle = (endAngle) % 360;  # Readjust
            totalMarks = getMarksBetweenAngles(startAngle, endAngle, dropThreshold, data)
            angleList.append((startAngle + ((45 / 2) * dir), totalMarks))
    print "Angle Data:"
    print angleList
    # print sorted(angleList, key=lambda (angle, totalMarks): totalMarks)

    """
    for i in range(0, 4):
      if(angleList[i][1] < 4):
        if(angleList[i][0] > 180):
          return (angleList[i][0] - 180) * -1
        else:
          return angleList[i][0]
    """
    return sorted(angleList, key=lambda (angle, totalMarks): totalMarks)[0][1]


# Draw simple lines to represent the X and Y axes
def drawAxes():
    # X Axis
    C.create_line(
        0,
        math.floor(HEIGHT / 2) - 2,
        WIDTH,
        math.floor(HEIGHT / 2) + 2,
        fill="green"
    )
    # Y Axis
    C.create_line(
        math.floor(WIDTH / 2) - 2,
        0,
        math.floor(WIDTH / 2) + 2,
        HEIGHT,
        fill="green"
    )
    for i in range(0, 6):
        # X Axis Markings
        x = math.floor(WIDTH / 2) + (i * 10 * PIXELS_PER_CM)
        yAxis = math.floor(HEIGHT / 2)
        C.create_line(
            x,
            yAxis - 10,
            x,
            yAxis + 10,
            fill="green"
        )
        x = math.floor(WIDTH / 2) + (-1 * i * 10 * PIXELS_PER_CM)
        C.create_line(
            x,
            yAxis - 10,
            x,
            yAxis + 10,
            fill="green"
        )
        # Y Axis Markings
        y = math.floor(HEIGHT / 2) + (i * 10 * PIXELS_PER_CM)
        xAxis = math.floor(WIDTH / 2)
        C.create_line(
            xAxis - 10,
            y,
            xAxis + 10,
            y,
            fill="green"
        )
        y = math.floor(HEIGHT / 2) + (-1 * i * 10 * PIXELS_PER_CM)
        xAxis = math.floor(WIDTH / 2)
        C.create_line(
            xAxis - 10,
            y,
            xAxis + 10,
            y,
            fill="green"
        )


# If the middle of the window was coordinate (x, y), draw the following point on the window
def drawPoint(x, y):
    xLoc = (WIDTH / 2) + x
    yLoc = (HEIGHT / 2) + y
    C.create_oval(xLoc - 1, yLoc - 2, xLoc + 2, yLoc + 2)


def drawLineFromOrigin(angle, distance):
    angle = angle * (-1)
    origin = (WIDTH / 2, HEIGHT / 2)
    destination = (distance * math.cos(math.radians(float(angle - 90))) * PIXELS_PER_CM,
                   distance * math.sin(math.radians(float(angle - 90))) * PIXELS_PER_CM)
    C.create_line(origin[0] - 1, origin[0] - 1, origin[0] + destination[0] + 1, origin[1] + destination[1] + 1,
                  fill="red")


# When we recieve scan debug data from the Arduino robot
# 'data' is expected to be a list containing 360 integers representing the distance at angle (where the index is the angle)
def onRecieveScan(data):
    global autoOn
    C.delete("all")
    drawAxes()
    for angle, distance in enumerate(data):
        drawPoint(
            math.floor(distance * math.cos(math.radians(float(angle + 90)))) * PIXELS_PER_CM,
            math.floor(distance * math.sin(math.radians(float(angle + 90)))) * (-1) * PIXELS_PER_CM
        )
    if (autoOn):
        myTuple = bestAngleToTurn(60, data)
        angle = myTuple[0]
        distance = myTuple[1]
        drawLineFromOrigin(angle, 40)
        print "Autonomous mode: Turning to angle ", angle, " then going ", distance, "cm forward"
        if (angle < 180):
            sendCommand("turn left ")
            sendCommand(str(angle))
            sendCommand("\n")
        else:
            sendCommand("turn right ")
            sendCommand(str(360 - angle))
            sendCommand("\n")
        sendCommand("go forward ")
        sendCommand(str(distance))
        sendCommand("\n")
        sendCommand("scan\n")


def processRemoteCommand(command):
    pass


def sendCommand(command):
    global sock
    if method == "usb":
        usb.write(command)
    elif method == "tcp":
        print "sending ", command
        sock.send(command)


def getCommandOverUSB():
    global usb
    if (usb.inWaiting() > 0):
        command = usb.readline().rstrip()
    else:
        command = ""
    return command

def getNextCommandFromBuffer():
    global tcpBuffer
    find = tcpBuffer.find("\n")
    if(find >= 0):
        command = str(tcpBuffer)[0:find]
        tcpBuffer = tcpBuffer[find : len(tcpBuffer) - find]
        return command
    else:
        return ""
"""
def getCommandOverTCP():
    input = ""
    global sock
    global tcpBuffer
    global client
    ready = select.select([sock], [], [], 0.001)
    if ready:
        input = sock.recv(1)
        tcpBuffer = tcpBuffer + input
        return getNextCommandFromBuffer()
    else:
        return ""
"""


def getCommandOverTCP():
    input = ""
    global sock
    global tcpBuffer
    global client
    try:
        msg = sock.recv(1)
        tcpBuffer += msg
    except socket.timeout:
        pass
    return getNextCommandFromBuffer()

def getCommand():
    global method
    if method == "usb":
        return getCommandOverUSB()
    elif method == "tcp":
        return getCommandOverTCP()

def task():
    global autoOn
    global tcpBuffer
    #print "."
    # Process input from STDIN
    if select.select([sys.stdin, ], [], [], 0.0)[0]:
        userCommand = sys.stdin.readline().rstrip()
        if userCommand == "auto on":
            print "Autonomous Mode On"
            sendCommand("scan\n")  # Initiate the back-and-forth communication
            autoOn = True
        elif userCommand == "scan":
            sendCommand("scan\n")  # Initiate the back-and-forth communication
        elif userCommand == "auto off":
            print "Autonomous Mode Off"
            autoOn = False
        elif userCommand == "calibrate":
            sendCommand("calibrate\n")  # Send calibration command
        elif (userCommand.startswith("turn ")):
            sendCommand(userCommand + "\n")
        elif (userCommand.startswith("go ")):
            sendCommand(userCommand + "\n")
        elif (userCommand.startswith("buffer")):
            print tcpBuffer
        else:
            #
            print "Unknown Command"

    command = getCommand()
    if command.startswith("scan "):
        print "Received Environment Scan from Arduino. Processing . . ."
        command = command[0:len(command) - 1]
        command = command.replace("scan ", "")
        convertedData = [int(x) for x in command.split(",")]
        onRecieveScan(convertedData)
    elif command.startswith("#"):
        print command

    window.after(1, task)  # reschedule event in 2 seconds


print "Running AGER Debugger"
print "Do you want to connect over USB or TCP? (enter 'usb' or 'tcp')"
method = sys.stdin.readline().rstrip()
if method == "usb":
    try:
        usb = serial.Serial('/dev/tty.usbmodem1411', 9600)
        print "Connected over USB"
    except serial.SerialException:
        print "Cannot connect over USB. Exiting."
        sys.exit()
elif method == "tcp":
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ('pi', 50000)
        sock.connect(server_address)
        sock.settimeout(0.01)
        print "Connected over TCP"
    except socket.error, msg:
        print "Cannot connect over TCP. Exiting."
        sys.exit()

C = Canvas(window, bg="blue", height=HEIGHT, width=WIDTH)
C.pack()
window.title("AGER Python Debugger")
window.after(10, task)
window.mainloop()
