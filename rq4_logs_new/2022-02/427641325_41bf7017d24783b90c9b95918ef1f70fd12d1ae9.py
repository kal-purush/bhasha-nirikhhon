#!/usr/bin/python3

import webbrowser
from random import randint, choice
import string
from tkinter import *

def open_my_channel():
    webbrowser.open_new("https://www.twitch.tv/art_hur421")

def generate_password():
    password_min = 10
    password_max = 20
    all_chars = string.ascii_letters + string.punctuation + string.digits

    password = "".join(choice(all_chars) for x in range (randint(password_min, password_max)))
    password_entry.delete(0, END)
    password_entry.insert(0, password)

def app():
    # crée la fenetre
    window = Tk()
    window.title("Générateur de mot de passe")
    window.geometry("720x480")
    window.minsize(720, 480)
    window.maxsize(720, 480)
    #window.iconbitmap("logo_A.ico")
    window.config(background='#4065A4')

    # crée la frame princip
    frame = Frame(window, bg='#4065A4')

    # création d'image
    width = 300
    height = 300
    image = PhotoImage(file='cyber-security1.png').zoom(35).subsample(32)
    canvas = Canvas(frame, width=width, height=height, bg='#4065A4', bd=0, highlightthickness=0)
    canvas.create_image(width/2, height/2, image=image)
    canvas.grid(row=0, column=0, sticky=W)

    # creer une sous boite
    right_frame = Frame(frame, bg='#4065A4')

    # creer un titre
    label_title = Label(right_frame, text="Mot de passe", font=("Helvetica", 20), bg='#4065A4', fg='white')
    label_title.pack()

    # creer un input
    password_entry = Entry(right_frame, font=("Helvetica", 20), bg='#4065A4', fg='white')
    password_entry.pack()

    # creer un bouton
    generate_password_button = Button(right_frame, text="Générer", font=("Helvetica", 20), fg='black', command=generate_password)
    generate_password_button.pack(fill=X)

    # on place la sous boite a droite de la frame principale
    right_frame.grid(row=0, column=1, sticky=W)

    # affciher la frame
    frame.pack(expand=YES)

    # creation d'une barre de menu
    menu_bar = Menu(window)
    # creer un premier menu
    file_menu = Menu(menu_bar, tearoff=0)
    file_menu.add_command(label="Nouveau", command=generate_password)
    file_menu.add_command(label="Quitter", command=window.quit)
    menu_bar.add_cascade(label="Fichier", menu=file_menu)

    # creer un second menu
    file_menu2 = Menu(menu_bar,tearoff=0)
    file_menu2.add_command(label="Art_hur421", command=open_my_channel)
    menu_bar.add_cascade(label="Crédit", menu=file_menu2)
    # configurer la fenetre pour ajouter cette menu bar
    window.config(menu=menu_bar)

    # afficher la fenetre
    window.mainloop()

app()