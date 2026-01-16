import numpy as np
import matplotlib.pyplot as plt
import math

def packetize(row):
    split = row.split(',')
    
    ret = {}

    ret["time"] = int(split[0])
    ret["x1"] = float(split[1])
    ret["y1"] = float(split[2])
    ret["z1"] = float(split[3])
    ret["x2"] = float(split[4])
    ret["y2"] = float(split[5])
    ret["z2"] = float(split[6])
    ret["x3"] = float(split[7])
    ret["y3"] = float(split[8])
    ret["z3"] = float(split[9])
    ret["hr"] = split[10]

    return ret

if __name__ == "__main__":
    path = './data.txt'
    data = np.loadtxt(path, dtype=str)
    # print(data)

    packets = []

    a1 = {"x": [], "y": [], "z": [], "mag": []}
    a2 = {"x": [], "y": [], "z": [], "mag": []}
    a3 = {"x": [], "y": [], "z": [], "mag": []}

    # z1 - z2 - x3 = x glob
    # y1 - y2 + y3 = y glob
    # -x1 - x2 + z3 = z glob

    x_glob = []
    y_glob = []
    z_glob = []
    mag_glob = []

    roll = [] # r1 left pos
    pitch = [] # r2 face down pos
    yaw = [] # r3 right rot pos
    rot_glob = []

    hockey_rad = 8
    football_rad = 10

    # r1 = ((x3 + x1 - x2) / 10cm) * 9.81 = r1 radians/s2
    # r2 = ((y1 - y2 - y3) / 10cm) * 9.81 = r2 radians/s2
    # r3 = ((y1 + y2) / 10cm) * 9.81 = r3 radians/s2

    t = []

    for d in data:
        packet = packetize(d)
        a1["x"].append(packet["x1"])
        a1["y"].append(packet["y1"])
        a1["z"].append(packet["z1"]) 


        a2["x"].append(packet["x2"])
        a2["y"].append(packet["y2"])
        a2["z"].append(packet["z2"]) 

        a3["x"].append(packet["x2"])
        a3["y"].append(packet["y2"])
        a3["z"].append(packet["z2"]) 

        t.append(packet["time"] / 1000)

        x_g = (packet["z2"] - packet["z1"] - packet["x3"]) / 3
        y_g = (packet["y2"] - packet["y1"] + packet["y3"]) / 3
        z_g = ((-1 * packet["x2"]) - packet["x1"] + packet["z3"]) / 3

        x_glob.append(x_g)
        y_glob.append(y_g)
        z_glob.append(z_g)
        mag_glob.append(math.sqrt(x_g**2 + y_g**2 + z_g**2))

        roll_i = ((packet["x3"] + packet["x2"] - packet["x1"]) / (3*10)) * 9.81
        pitch_i = ((packet["y2"] - packet["y1"] - packet["y3"]) / (3*10)) * 9.81
        yaw_i = ((packet["y2"] + packet["y1"]) / (2*10)) * 9.81

        roll.append(roll_i)
        pitch.append(pitch_i)
        yaw.append(yaw_i)
        rot_glob.append(math.sqrt(roll_i**2 + pitch_i**2 + yaw_i**2))


        packets.append(packet)
    '''
    plt.plot(t, a1.get('x')) # blue
    plt.plot(t, a1.get('y')) # orange
    plt.plot(t, a1.get('z')) # green

    plt.xlabel('time')
    plt.ylabel('accel')
    plt.title('a1')
    plt.show()

    plt.plot(t, a2.get('x')) # blue
    plt.plot(t, a2.get('y')) # orange
    plt.plot(t, a2.get('z')) # green

    plt.xlabel('time')
    plt.ylabel('accel')
    plt.title('a2')
    plt.show()

    plt.plot(t, a3.get('x')) # blue
    plt.plot(t, a3.get('y')) # orange
    plt.plot(t, a3.get('z')) # green
    '''
    ax = plt.gca()
    ax.set_ylim([-5, 5])
    ax.plot(t, roll)
    ax.plot(t, pitch)
    ax.plot(t, yaw)
    ax.plot(t, rot_glob)
    plt.xlabel("time")
    plt.ylabel("rot accel")
    plt.show()

    plt.plot(t, x_glob)
    plt.plot(t, y_glob)
    plt.plot(t, z_glob)
    plt.plot(t, mag_glob)
    plt.xlabel("Time")
    plt.ylabel("Global Acc")
    plt.show()

    x1List = []
    x2List = []
    x3List = []

    x1List.append(a1.get('x'))
    x2List.append(a2.get('x'))
    x3List.append(a3.get('x'))

    x1List = x1List[0]
    x2List = x2List[0]
    x3List = x3List[0]
    xDat = [x1List, x2List, x3List]
    print(np.shape(xDat))

    xMag = []

    for i in range(len(x1List)):
        xMag.append(math.sqrt(x1List[i]**2 + x2List[i]**2 + x3List[i]**2))

    y1List = []
    y2List = []
    y3List = []

    y1List.append(a1.get('y'))
    y2List.append(a2.get('y'))
    y3List.append(a3.get('y'))

    y1List = y1List[0]
    y2List = y2List[0]
    y3List = y3List[0]
    yDat = [y1List, y2List, y3List]
    print(np.shape(yDat))

    yMag = []

    for i in range(len(y1List)):
        yMag.append(math.sqrt(y1List[i] + y2List[i] + y3List[i]))

    z1List = []
    z2List = []
    z3List = []

    z1List.append(a1.get('z'))
    z2List.append(a2.get('z'))
    z3List.append(a3.get('z'))

    z1List = z1List[0]
    z2List = z2List[0]
    z3List = z3List[0]
    zDat = [z1List, z2List, z3List]
    print(np.shape(zDat))

    zMag = []

    for i in range(len(z1List)):
        zMag.append(math.sqrt(z1List[i] + z2List[i] + z3List[i]))

    mag = []
    for i in range(len(xMag)):
        mag.append()

    plt.xlabel('time')
    plt.ylabel('Magnitude')
    plt.plot(t, xMag)
    plt.plot(t, yMag)
    plt.plot(t, zMag)
    plt.show()
    '''
    plt.title('a3')
    plt.show()
    '''
