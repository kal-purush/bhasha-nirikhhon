
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

import matplotlib.pyplot as plt
import numpy as np

from dotenv import dotenv_values

if __name__ == "__main__":
    # Fetch the service account key JSON file contents
    config = dotenv_values(".env")
    path = config["PATH"]
    # print(path)
    cred = credentials.Certificate(path)

    # Initialize the app with a service account, granting admin privileges
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://nu-capstone-db-default-rtdb.firebaseio.com/'
    })

    sheet = '/impact_testing_throw/4faf183e-1fb5-459e-8fcc-c5c9c331914b/'
    ref = db.reference(sheet)
    
    test_ref = '/impact_testing_throw/'
    helmets = test_ref.order_by_key().get()

    times = ref.order_by_key().get()

    # print(times) 
    
    a1 = {"x": [], "y": [], "z": []}
    a2 = {"x": [], "y": [], "z": []}
    a3 = {"x": [], "y": [], "z": []}
    t = []
    i = 0

    for key, value in times.items():
        t.append(i)
        i += 1
        a1.get("x").append(value.get("x1"))
        a1.get("y").append(value.get("y1"))
        a1.get("z").append(value.get("z1"))

        a2.get("x").append(value.get("x2"))
        a2.get("y").append(value.get("y2"))
        a2.get("z").append(value.get("z2"))
        
        a3.get("x").append(value.get("x3"))
        a3.get("y").append(value.get("y3"))
        a3.get("z").append(value.get("z3"))

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