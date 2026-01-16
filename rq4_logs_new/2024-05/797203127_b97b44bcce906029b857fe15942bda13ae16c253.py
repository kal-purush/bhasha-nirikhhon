from structure import *
import time
import numpy as np
import matplotlib.pyplot as plt

moto = Moto(0.3, 0.5, 400, 2, 5)
x = []
y = []

test = []
    
init_time = time.time()
current_time = 0

while current_time < 10:

    previous_time = current_time
    current_time = time.time() - init_time
    dt = current_time-previous_time

    moto.acceleration = 0

    if current_time > 1 and current_time < 5:
        moto.inclinaison = np.pi/3 * (current_time-1) * 0.1
    elif current_time > 5 and current_time < 8:
        moto.inclinaison = np.pi/3 * 0.4 - np.pi/3 * 0.4/3 * (current_time-5)
    elif current_time > 8 : 
        moto.inclinaison = 0
    # print('vit : ', moto.vitesse)
    # print('braq : ', moto.braquage_avant)
    # print('dt : ', dt)
    # print(' ')

    moto.update_inclinaison(dt)
    moto.update_vitesse(dt)
    moto.update_position(dt)
    moto.calcul_rayon_courbure()
    moto.calcul_vitesse_angle()
    moto.update_angle(dt)
    moto.calcul_rotation_matrice()
    pos = moto.changement_repere(moto.position_moto)
    x.append(pos[0])
    y.append(pos[1])

    #print(moto.rayon_courbure)

    time.sleep(0.1)


plt.figure()
#plt.plot(range(len(test)), test)
plt.plot(x, y)
plt.show()