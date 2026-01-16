"""
Projekt rulett
tkinteri abil lihtsustatud ruleti simulatsioon
"""

import tkinter as tk
from tkinter import ttk


def arvuta_panused():

    panusestring = ""

    for p in panused:
        panusestring += str(nupud[p]) + " Panus:" + str(panused[p]) + "\n"
    return panusestring

    
def vasak_hiireklahv(event, nupp, nupu_kirje):
    nupp.config(fg='blue')
    panused[nupp] = panused.get(nupp, 0) + bet
    panuseinfo.set(arvuta_panused())
    print(panused.get(nupp, "Tühi"))
    
def parem_hiireklahv(event, nupp, nupu_kirje):
    nupp.config(fg='red')
    if panused.get(nupp, 0):
        panused[nupp] = panused.get(nupp, 0) - bet
    else:
        panused.pop(nupp, None)
    panuseinfo.set(arvuta_panused())
    print(panused.get(nupp, "Tühi"))


def create_button(frame, text):

    nupp = tk.Button(frame, text=text)
    nupp.bind('<Button-1>', lambda event, obj=nupp: vasak_hiireklahv(event, nupp, nupud[nupp]))
    nupp.bind('<Button-3>', lambda event, obj=nupp: parem_hiireklahv(event, nupp, nupud[nupp]))
    return nupp

    
root = tk.Tk()

root.title("Rulett")
tabloo = ttk.Frame(root)
ruletilaud = ttk.Frame(root)
panuseframe = ttk.Frame(root)
panusenupud = ttk.Frame(root)
animatsioon = ttk.Frame(root)

animatsioon.grid(column=0, row=0)
tabloo.grid(column=0, row=1)
ruletilaud.grid(column=1, row=2)
panuseframe.grid(column=2, row=1, rowspan=5)
panusenupud.grid(column=1, row=3)


tablooinfo = ttk.Label(tabloo, text='Tablooinfo')
tablooinfo.grid(column=0, row=0)



# Ruletilaua nupud
nupp_0_frame = ttk.Frame(ruletilaud)
nupp_0_frame.grid(column=0, row=0)
nupp_0 = create_button(nupp_0_frame, text="0")
nupp_0.grid(column=0, row=0, rowspan=3)
nupp_0.config(height = 4, width=1)

nupp_1_36_frame = ttk.Frame(ruletilaud)
nupp_1_36_frame.grid(column=1, row=0)

nupp_1 = create_button(nupp_1_36_frame, text="1")
nupp_2 = create_button(nupp_1_36_frame, text="2")
nupp_3 = create_button(nupp_1_36_frame, text="3")
nupp_4 = create_button(nupp_1_36_frame, text="4")
nupp_5 = create_button(nupp_1_36_frame, text="5")
nupp_6 = create_button(nupp_1_36_frame, text="6")
nupp_7 = create_button(nupp_1_36_frame, text="7")
nupp_8 = create_button(nupp_1_36_frame, text="8")
nupp_9 = create_button(nupp_1_36_frame, text="9")
nupp_10 = create_button(nupp_1_36_frame, text="10")
nupp_11 = create_button(nupp_1_36_frame, text="11")
nupp_12 = create_button(nupp_1_36_frame, text="12")
nupp_13 = create_button(nupp_1_36_frame, text="13")
nupp_14 = create_button(nupp_1_36_frame, text="14")
nupp_15 = create_button(nupp_1_36_frame, text="15")
nupp_16 = create_button(nupp_1_36_frame, text="16")
nupp_17 = create_button(nupp_1_36_frame, text="17")
nupp_18 = create_button(nupp_1_36_frame, text="18")
nupp_19 = create_button(nupp_1_36_frame, text="19")
nupp_20 = create_button(nupp_1_36_frame, text="20")
nupp_21 = create_button(nupp_1_36_frame, text="21")
nupp_22 = create_button(nupp_1_36_frame, text="22")
nupp_23 = create_button(nupp_1_36_frame, text="23")
nupp_24 = create_button(nupp_1_36_frame, text="24")
nupp_25 = create_button(nupp_1_36_frame, text="25")
nupp_26 = create_button(nupp_1_36_frame, text="26")
nupp_27 = create_button(nupp_1_36_frame, text="27")
nupp_28 = create_button(nupp_1_36_frame, text="28")
nupp_29 = create_button(nupp_1_36_frame, text="29")
nupp_30 = create_button(nupp_1_36_frame, text="30")
nupp_31 = create_button(nupp_1_36_frame, text="31")
nupp_32 = create_button(nupp_1_36_frame, text="32")
nupp_33 = create_button(nupp_1_36_frame, text="33")
nupp_34 = create_button(nupp_1_36_frame, text="34")
nupp_35 = create_button(nupp_1_36_frame, text="35")
nupp_36 = create_button(nupp_1_36_frame, text="36")


nupp_1.grid(column=0, row=0)
nupp_2.grid(column=0, row=1)
nupp_3.grid(column=0, row=2)
nupp_4.grid(column=1, row=0)
nupp_5.grid(column=1, row=1)
nupp_6.grid(column=1, row=2)
nupp_7.grid(column=2, row=0)
nupp_8.grid(column=2, row=1)
nupp_9.grid(column=2, row=2)
nupp_10.grid(column=3, row=0)
nupp_11.grid(column=3, row=1)
nupp_12.grid(column=3, row=2)

# Kas nuppude sõnastiku saaks kuidagi viisakamalt teha, see kõik siin läheb jube pikaks.
bet = 1
panused = dict()
nupud = dict()

nupud[nupp_0] = "Nupp 0"
nupud[nupp_1] = "Nupp 1"
nupud[nupp_2] = "Nupp 2"
nupud[nupp_3] = "Nupp 3"
nupud[nupp_4] = "Nupp 4"
nupud[nupp_5] = "Nupp 5"
nupud[nupp_6] = "Nupp 6"
nupud[nupp_7] = "Nupp 7"
nupud[nupp_8] = "Nupp 8"
nupud[nupp_9] = "Nupp 9"

panuseinfo = tk.StringVar()
show_bets = tk.Label(panuseframe, textvariable=panuseinfo)
show_bets.grid(column=0, row=0)

root.mainloop()

