#!/usr/bin/env python3
#-*- coding: utf-8 -*-
#"""
#Created on Wed May 6 11:45:46 2015

#@author: dani.ruiz pol.gomez
#"""

#Librerias#
import numpy as np
import random
from tkinter import *
#############import RPi.GPIO as GPIO
import time
#--------#
#GPIO#
#############GPIO.setmode(GPIO.BCM)
#############GPIO.setup(18, GPIO.IN, pull_up_down=GPIO.PUD_UP)
#--------#

#ventana principal#
window=Tk()
j,h = window.winfo_screenwidth(), window.winfo_screenheight()
#window.overrideredirect(1)
window.title("Sistema Indicador de Direcció per l'Anàlisis de Prevenció de Lesions de Lligaments Creuats Anteriors")
window.geometry("%dx%d+0+0" % (j,h))
window.config(bg="black")
#----------#

#variables#
puls=1
control=0
control2=0
temps=DoubleVar()
temps2=0.0
menu=0
control_saltar=0
control_correr_dre=0
control_correr_esq=0
control_correr_fre=0
control_salt=0
a=0
b=0
c=0
#Rectangle#
r11= 0
r12= 0
r21= j
r22= 0
r31= j
r32= h
r41= 0
r42= h
# Flecha Derecha
a1 = (j/8)*5
a2 = (h/4)
b1 = (j/4)*3
b2 = (h/2)
c1 = (j/8)*5
c2 = (h/4)*3
d1 = (j/8)*5
d2 = (h/8)*5
e1 = (j/4)
e2 = (h/8)*5
f1 = (j/4)
f2 = (h/8)*3
g1 = (j/8)*5
g2 = (h/8)*3
# Flecha Izquierda
a3 = (j/8)*3
a4 = (h/4)
b3 = (j/4)
b4 = (h/2)
c3 = (j/8)*3
c4 = (h/4)*3
d3 = (j/8)*3
d4 = (h/8)*5
e3 = (j/4)*3
e4 = (h/8)*5
f3 = (j/4)*3
f4 = (h/8)*3
g3 = (j/8)*3
g4 = (h/8)*3
# Flecha Recta
a5 = (j/2)
a6 = (h/8)
b5 = (j/8)*5
b6 = (h/8)*3
c5 = (j/16)*9
c6 = (h/8)*3
d5 = (j/16)*9
d6 = (h/8)*6
e5 = (j/16)*7
e6 = (h/8)*6
f5 = (j/16)*7
f6 = (h/8)*3
g5 = (j/8)*3
g6 = (h/8)*3
#----------#
#colores
color=["red","yellow","green","blue"]
#----------#

def menu_jugar_fre():
	global control,area_jugar,puls
	rand = np.random.choice([1,2],p=[0.5,0.5])
	if control != 1:
		area_jugar = Canvas(window, width=j, height=h)
		area_jugar.config(bg="black",highlightbackground="black")
		area_jugar.pack(fill=BOTH)
		control = 1
		if rand == 1:
			area_jugar.create_polygon(a1,a2,b1,b2,c1,c2,d1,d2,e1,e2,f1,f2,g1,g2, fill = "yellow")
		if rand == 2:
			area_jugar.create_polygon(a3,a4,b3,b4,c3,c4,d3,d4,e3,e4,f3,f4,g3,g4, fill ="yellow")
		puls = 0

def menu_jugar_dre():
	global control,area_jugar,puls
	rand = np.random.choice([1,2],p=[0.7,0.3])
	if control != 1:
		area_jugar = Canvas(window, width=j, height=h)
		area_jugar.config(bg="black",highlightbackground="black")
		area_jugar.pack(fill=BOTH)
		control = 1
		if rand == 1:
			area_jugar.create_polygon(a1,a2,b1,b2,c1,c2,d1,d2,e1,e2,f1,f2,g1,g2, fill = "yellow")         
		if rand == 2:
			area_jugar.create_polygon(a5,a6,b5,b6,c5,c6,d5,d6,e5,e6,f5,f6,g5,g6, fill ="yellow")
		puls = 0

def menu_jugar_esq():
	global control,area_jugar,puls
	rand = np.random.choice([1,2],p=[0.7,0.3])
	if control != 1:
		area_jugar = Canvas(window, width=j, height=h)
		area_jugar.config(bg="black",highlightbackground="black")
		area_jugar.pack(fill=BOTH)
		control = 1
		if rand == 1:
			area_jugar.create_polygon(a5,a6,b5,b6,c5,c6,d5,d6,e5,e6,f5,f6,g5,g6, fill = "yellow")
		if rand == 2:
			area_jugar.create_polygon(a3,a4,b3,b4,c3,c4,d3,d4,e3,e4,f3,f4,g3,g4, fill ="yellow")
		puls = 0

def menu_jugar_salt1():
	global control,control_salt,area_jugar_salt,a,b,c
	area_jugar_salt = Canvas(window, width=j, height=h)
	area_jugar_salt.pack(fill=BOTH)
	if control_salt==0:
#### color1 ####
		color1=area_jugar_salt.create_polygon(r11,r12,r21,r22,r31,r32,r41,r42, fill=color[a])
		area_jugar_salt.after(300,desaparece_salt)
	if control_salt==1:
#### color2 ####
		color2=area_jugar_salt.create_polygon(r11,r12,r21,r22,r31,r32,r41,r42, fill=color[b])
		area_jugar_salt.after(400,desaparece_salt)
	if control_salt==2:
#### color3 #### 
		color3=area_jugar_salt.create_polygon(r11,r12,r21,r22,r31,r32,r41,r42, fill=color[c])
		area_jugar_salt.after(600,desaparece_salt)
	control_salt = control_salt + 1

def opcio_saltar2():
	global control_salt,a,b,c
	control_salt=0
	a=random.randrange(4)
	b=random.randrange(4)
	c=random.randrange(4)
	while b==a:
		b=random.randrange(4)
	while c==a or c==b:
		c=random.randrange(4)
	menu_jugar_salt1()

def desaparece():
	global area_jugar, control,control2
	area_jugar.destroy()
	control = 0
	control2 = 0

def desaparece_salt():
	global area_jugar_salt, control_salt,numeros
	area_jugar_salt.pack_forget()
	if control_salt<3:
		menu_jugar_salt1()

def activar_fre():
	global puls,temps2
	while puls == 1:
		input_state = GPIO.input(18)
		if input_state == False:
			menu_jugar_fre()
			time.sleep(temps2)
	area_jugar.after(2000,desaparece)
	puls = 1

def activar_dre():
	global puls,temps2
	while puls == 1:
		input_state = GPIO.input(18)
		if input_state == False:
			menu_jugar_dre()
			time.sleep(temps2)
	area_jugar.after(2000,desaparece)	
	puls = 1

def activar_esq():
	global puls,temps2
	while puls == 1:
		input_state = GPIO.input(18)
		if input_state == False:
			menu_jugar_esq()
			time.sleep(temps2)
	area_jugar.after(2000,desaparece)
	puls = 1
	
def validar_temps():
	global temps2
	temps2=temps.get()

def menu_correr_fre():
	global menu, menu_correr, control_correr_fre
	if control_correr_fre != 1:
		menu_correr=Canvas(window, width=500, height=25)
		menu_correr.config(bg="black", highlightbackground="black")
		menu_correr.pack()
		l1=Label(menu_correr, text="Has escollit Correr i Frenada").pack()
		començar_corre=Button(menu_correr, text="Començar", command=menu_jugar_fre).pack(side=LEFT)
		sortir_correr=Button(menu_correr, text="Sortir", command=exit).pack(side=RIGHT)
		temps_correr=Button(menu_correr, text="Temps", command=validar_temps).pack(side=RIGHT)
		caixa_text=Entry(menu_correr, width=5, textvariable=temps).pack(side=RIGHT)
		menu=1
		control_correr_fre=1

def menu_correr_dre():
	global menu, menu_correr, control_correr_dre
	if control_correr_dre != 1:
		menu_correr=Canvas(window, width=500, height=25)
		menu_correr.config(bg="black", highlightbackground="black")
		menu_correr.pack()
		l1=Label(menu_correr, text="Has escollit Correr a la dreta").pack()
		començar_corre=Button(menu_correr, text="Començar", command=activar_dre).pack(side=LEFT)
		sortir_correr=Button(menu_correr, text="Sortir", command=exit).pack(side=RIGHT)
		temps_correr=Button(menu_correr, text="Temps", command=validar_temps).pack(side=RIGHT)
		caixa_text=Entry(menu_correr, width=5, textvariable=temps).pack(side=RIGHT)
		menu=2
		control_correr_dre=1
		
def menu_correr_esq():
	global menu, menu_correr, control_correr_esq
	if control_correr_esq != 1:
		menu_correr=Canvas(window, width=500, height=25)
		menu_correr.config(bg="black", highlightbackground="black")
		menu_correr.pack()
		l1=Label(menu_correr, text="Has escollit Correr a la esquerra").pack()
		començar_corre=Button(menu_correr, text="Començar", command=activar_esq).pack(side=LEFT)
		sortir_correr=Button(menu_correr, text="Sortir", command=exit).pack(side=RIGHT)
		temps_correr=Button(menu_correr, text="Temps", command=validar_temps).pack(side=RIGHT)
		caixa_text=Entry(menu_correr, width=5, textvariable=temps).pack(side=RIGHT)
		menu=3
		control_correr_esq=1

def menu_saltar2():
	global menu, menu_saltar, control_saltar,menu_jugar_salt
	if control_saltar != 1:
		menu_saltar=Canvas(window, width=500, height=25)
		menu_saltar.config(bg="blue", highlightbackground="blue")
		menu_saltar.pack()
		l2=Label(menu_saltar, text="Has escollit Saltar").pack()
		jugar2=Button(menu_saltar, text="Començar", command=opcio_saltar2).pack(side=LEFT)
		salir2=Button(menu_saltar, text="Sortir", command=exit).pack(side=RIGHT)
		menu=4
		control_saltar=1

def opcio_correr_fre():
	global menu, menu_correr, menu_saltar, control_saltar, control_correr_dre, control_correr_esq
	control_saltar=0
	control_correr_dre=0
	control_correr_esq=0
	if menu == 2 or menu == 3:
		menu_correr.pack_forget()
		menu_correr.destroy()
		menu_correr_fre()
	elif menu == 4:
		menu_saltar.pack_forget()
		menu_saltar.destroy()
		menu_correr_fre()
	else:
		menu_correr_fre()

def opcio_correr_dre():
	global menu, menu_correr, menu_saltar, control_saltar, control_correr_fre, control_correr_esq
	control_saltar=0
	control_correr_fre=0
	control_correr_esq=0
	if menu == 1 or menu == 3:
		menu_correr.pack_forget()
		menu_correr.destroy()
		menu_correr_dre()
	elif menu == 4:
		menu_saltar.pack_forget()
		menu_saltar.destroy()
		menu_correr_dre()
	else:
		menu_correr_dre()

def opcio_correr_esq():
	global menu, menu_correr, menu_saltar, control_saltar, control_correr_dre, control_correr_fre
	control_saltar=0
	control_correr_fre=0
	control_correr_dre=0
	if menu==1 or menu==2:
		menu_correr.pack_forget()
		menu_correr.destroy()
		menu_correr_esq()
	elif menu==4:
		menu_saltar.pack_forget()
		menu_saltar.destroy()
		menu_correr_esq()	
	else:
		menu_correr_esq()

def opcio_saltar():
	global menu, menu_correr, menu_saltar, control_saltar,control_correr_esq, control_correr_dre, control_correr_fre
	control_correr_dre=0
	control_correr_esq=0
	control_correr_fre=0
	if menu == 1 or menu == 2 or menu == 3:
		menu_correr.pack_forget()
		menu_correr.destroy()
		menu_saltar2()
	else:
		menu_saltar2()

# menu_principal#
menu1=Menu(window)
window.config(menu=menu1)
menu1_1=Menu(menu1, tearoff=0)
menu1.add_cascade(label="Modalitat de Joc", menu=menu1_1)
menu1_1_1=Menu(menu1_1, tearoff=0)
menu1_1.add_command(label="Frenada", command=opcio_correr_fre)
menu1_1.add_command(label="Correr Dreta", command=opcio_correr_dre)
menu1_1.add_command(label="Correr Esquerra", command=opcio_correr_esq)
menu1_1.add_command(label="Saltar", command=opcio_saltar)
menu1_1.add_command(label="Sortir", command=exit)

#---------#

window.mainloop()
