######### 1) Importation ##############################
from tkinter import *


######### 2) Fonctions ################################
def moove() :
    global x, can,bal,fen

    x = x + 50
    can.coords(bal, x, 200, x + 50, 250)    
    fen.after(500, moove)                   # appeler à nouveau la fonction moove 500 ms plus tard


######## 3) Main ######################################

def test():
    global x, can,bal,fen
    # Création de la fenêtre et du canevas :
    fen = Tk()
    can = Canvas(fen, width = 600, height = 450, bg = 'ivory')
    can.pack()

    # Création de l'abscisse "x" de la balle et de la balle "bal" :
    x = 0
    bal = can.create_line(x, 200, x + 50, 250, fill='blue')

    # On lance la fonction d'animation :
    moove()

    # On cède la main au réceptionnaire d'événements :
    fen.mainloop()