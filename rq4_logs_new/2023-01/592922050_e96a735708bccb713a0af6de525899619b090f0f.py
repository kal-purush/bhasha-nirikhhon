import picar_4wd as fc
import random
import time


def check_threat(list_threats):
    # [2:6] represents the degree range of -30 to 30
    for distance in list_threats[2:6]:
        # no obstacle_detected
        if distance == -2:
            continue
        elif distance < 20:
            fc.stop()
            return True

    # Checks peripherals, makes less sensitive than direct
    if list_threats[1] != -2:
        if list_threats[1] < 12:
            fc.stop()
            return True
    if list_threats[7] != -2:
        if list_threats[7] < 12:
            fc.stop()
            return True
    return False
# if a threat was detected this function moves the car backwards, then randomly chooses left of right

def avoid_threat(isThreatDetected):
    if (isThreatDetected):
        fc.backward(1)
        time.sleep(1)
        fc.stop()

        if random.randint(0,100) > 50:
            fc.turn_right(1)
            time.sleep(0.5)
            fc.stop()
        else:
            fc.turn_left(1)
            time.sleep(0.5)
            fc.stop()

def drive():
    # 20 is an arbitrary amount that just allows the program to terminate after ~20-30 seconds
    for k in range(20):
        fc.forward(1)
        # list to store distance to objects at each angle
        threats = []
        # gets distance of all potential objects scanning from servo -60 to 60 degrees
        for i in range (-60, 61, 15):
            threats.append(fc.get_distance_at(i))
        avoid_threat(check_threat(threats))

        # list to store distance to objects at each
        threats = []

        # gets distance of all potential objects scanning from servo 60 to -60 degrees
        for j in range (60, -61, -15):
            threats.append(fc.get_distance_at(j))
        avoid_threat(check_threat(threats))
    fc.stop()

if __name__ == '__main__':
    drive()