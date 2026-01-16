import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# Definiere die Funktion f(x, y)
def f(x, y):
    return x**2 + y**2

# Erstelle ein Gitter für x und y
x = np.linspace(0, 2, 100)
y = np.linspace(0, 2, 100)
X, Y = np.meshgrid(x, y)

# Berechne die Werte von f(x, y) für jeden Punkt im Gitter
Z = f(X, Y)

# Erstelle die 3D-Darstellung
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

# Zeichne die durchsichtige Flächenfunktion
ax.plot_surface(X, Y, Z, alpha=0.5)

# Zeichne die Wand entlang der x-Achse, die an jedem Punkt (x, 0) gerade f(x, 0) hoch ist
x_wall = np.linspace(0, 2, 100)
y_wall = np.zeros_like(x_wall)
z_wall = f(x_wall, y_wall)

X_wall, Z_wall = np.meshgrid(x_wall, z_wall)
Y_wall = np.zeros_like(X_wall)

for i in range(len(x_wall)):
    ax.plot([x_wall[i], x_wall[i]], [0, 0], [0, z_wall[i]], color='red', lw=1)

# Füge die Beschriftungen hinzu
label_x = 1
label_y = 0.2
label_z = f(label_x, label_y) + 1
ax.text(label_x, label_y, label_z, 'f(x, y)', fontsize=12)

label_x = 1.1
label_y = -0.4
label_z = f(label_x, 0) / 2
ax.text(label_x, label_y, label_z, 'Inneres Integral', fontsize=12)

# Setze die Achsenbeschriftungen
ax.set_xlabel('Y')
ax.set_ylabel('X')
ax.set_zlabel('Z')

# Zeige die Darstellung
plt.show()