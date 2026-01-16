import tkinter as tk
#import numpy as np
import math
import random

WIDTH = 600
HEIGHT = 600
theta = (-1) * math.pi / 2
temps = 50
rayon_cercle = (WIDTH - 200) / 2

#COULEURS (c'est juste pour le style)
LISTE_COULEUR =["#D6ECFF", "#CCE8FF", "#C5E5FF", "#BFE2FF", "#B8DFFF", "#A8D7FF", "#A1D4FF", "#95CFFF", "#8ACAFF", "#80C5FF"]

root = tk.Tk()
root.title("aaaaaaaaaaaaaaaaaaa")

screen = tk.Canvas(root, height=HEIGHT, width=WIDTH, bg="black")
cercle = screen.create_oval(100, 100, WIDTH - 100, HEIGHT - 100, outline="#2FA0FF")
taille_objet = 20
objet_temps = screen.create_oval((WIDTH/2) - (taille_objet/2), (HEIGHT - 100) - (taille_objet/2), (WIDTH/2) + (taille_objet/2), (HEIGHT - 100) + (taille_objet/2), fill="#EAF3FB", outline="#A2B5C7")

def coord_temps(theta):
    x = rayon_cercle * math.cos(theta)
    y = rayon_cercle * math.sin(theta)

    return x, y

def move():
    global theta

    theta += math.pi / temps
    dx = (-1) * (coord_temps(theta)[0] - coord_temps(theta - math.pi / temps)[0])
    dy = (-1) * (coord_temps(theta)[1] - coord_temps(theta - math.pi / temps)[1])
    screen.move(objet_temps, dx, dy)

    screen.after(40, move)


def trace_polyrythme(NOMBRE_DE_RYTHME):
    liste_points = []
    alpha = math.pi / 2
    color = random.choice(LISTE_COULEUR)

    while alpha <= (2 * math.pi) + (math.pi / 2):
        liste_points.append(coord_temps(alpha))
        alpha += (2 * math.pi) / NOMBRE_DE_RYTHME
    print(liste_points)
    for i in range(len(liste_points)):
        screen.create_line((WIDTH/2) + liste_points[i-1][0], (HEIGHT/2) + liste_points[i-1][1], (WIDTH/2) + liste_points[i][0], (HEIGHT/2) + liste_points[i][1], fill=color)

move()
for i in range(3, 10):
    trace_polyrythme(i)

screen.grid(column=0, row=0, rowspan=4)

root.mainloop()