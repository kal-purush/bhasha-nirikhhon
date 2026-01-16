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
    
    t = []

    for d in data:
        packet = packetize(d)
        a1["x"].append(packet["x1"])
        a1["y"].append(packet["y1"])
        a1["z"].append(packet["z1"]) 
    
        a1["mag"].append() 

        a2["x"].append(packet["x2"])
        a2["y"].append(packet["y2"])
        a2["z"].append(packet["z2"]) 

        a3["x"].append(packet["x2"])
        a3["y"].append(packet["y2"])
        a3["z"].append(packet["z2"]) 

        t.append(packet["time"])
        packets.append(packet)
    
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

    plt.xlabel('time')
    plt.ylabel('accel')
    plt.title('a3')
    plt.show()





