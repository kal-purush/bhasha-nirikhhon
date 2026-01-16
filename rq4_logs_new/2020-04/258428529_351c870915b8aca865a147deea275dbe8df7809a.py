# -*- coding: utf-8 -*-

import string
import random
from colorama import Back, Fore, init

init(autoreset=True)

def passs():
    while True:
        print '''
####    ###     ####    #### 
#   #  #   #   #    #  #    #
#   # #     #  #       #     
####  #######   ####    ####
#     #     # #     # #     #
#     #     #  #####   #####
Version:2.1
Codigo escrito por: Angelo Mass - Darko.

1) Contrasena de mayusculas.
2) Contrasena de solo minusculas.
3) Contrasena de solo numeros.
4) Contrasena con mayusculas, minusculas y numeros.
5) Contrasena con mayusculas, minusculas, numeros y caracteres especiales.
6) Salir.
'''
        opcion = raw_input('Darko@Pass$ ')
        if opcion == '1':
            mayusculas()
            break
        elif opcion == '2':
            minusculas()
            break
        elif opcion == '3':
            numeros()
            break
        elif opcion == '4':
            mayus_minus_num()
            break
        elif opcion == '5':
            mixto()
            break
        elif opcion == '6':
            exit()
        else:
            print Fore.RED + '[*]Ingrese un argumento valido.'

def mayusculas():
    print '''
####    ###     ####    #### 
#   #  #   #   #    #  #    #
#   # #     #  #       #     
####  #######   ####    ####
#     #     # #     # #     #
#     #     #  #####   #####
Version:2.1
Codigo escrito por: Angelo Mass - Darko.
Seccion: mayusculas.
'''
    longitud = raw_input('Introduzca la longitud de su password: ')
    password = string.ascii_uppercase
    try:
        procesar = str(''.join(random.sample(password, int(longitud))))
        print Fore.GREEN +  '[*]Su password es: ' + procesar
        passs()
    except:
        print Fore.RED + '[*]Reintentar. El valor maximo permitido es 26. No utilizar caracteres alfabeticos.'
        passs()

def minusculas():
    print '''
####    ###     ####    #### 
#   #  #   #   #    #  #    #
#   # #     #  #       #     
####  #######   ####    ####
#     #     # #     # #     #
#     #     #  #####   #####
Version:2.1
Codigo escrito por: Angelo Mass - Darko.
Seccion: minusculas.
'''
    longitud = raw_input('Introduzca la longitud de su password: ')
    password = string.ascii_lowercase
    try:
        procesar = str(''.join(random.sample(password, int(longitud))))
        print Fore.GREEN + '[*]Su password es: ' + procesar
        passs()
    except:
        print Fore.RED + '[*]Reintentar. El valor maximo permitido es 26. No utilizar caracteres alfabeticos.'
        passs()

def numeros():
    print '''
####    ###     ####    #### 
#   #  #   #   #    #  #    #
#   # #     #  #       #     
####  #######   ####    ####
#     #     # #     # #     #
#     #     #  #####   #####
Version:2.1
Codigo escrito por: Angelo Mass - Darko.
Seccion: numeros.
'''
    longitud = raw_input('Introduzca la longitud de su password: ')
    password = string.digits
    try:
        procesar = str(''.join(random.sample(password, int(longitud))))
        print Fore.GREEN + '[*]Su password es: ' + procesar
	      passs()
    except:
	      print Fore.RED + '[*]Reintentar. El valor maximo permitido es 26. No utilizar caracteres alfabeticos.'
	      passs()

def mayus_minus_num():
    print '''
####    ###     ####    #### 
#   #  #   #   #    #  #    #
#   # #     #  #       #     
####  #######   ####    ####
#     #     # #     # #     #
#     #     #  #####   #####
Version:2.1
Codigo escrito por: Angelo Mass - Darko.
Seccion: mayusculas, minusculas y numeros.
'''
    longitud = raw_input('Introduzca la longitud de su password: ')
    password = string.ascii_lowercase + string.ascii_uppercase + string.digits
    try:
        procesar = str(''.join(random.sample(password, int(longitud))))
        print Fore.GREEN + '[*]Su password es: ' + procesar
        passs()
    except:
        print Fore.RED + '[*]Reintentar. El valor maximo permitido es 26. No utilizar caracteres alfabeticos.'
        passs()
def mixto():
    print '''
####    ###     ####    #### 
#   #  #   #   #    #  #    #
#   # #     #  #       #     
####  #######   ####    ####
#     #     # #     # #     #
#     #     #  #####   #####
Version:2.1
Codigo escrito por: Angelo Mass - Darko.
Seccion: mayusculas, minusculas, numeros y caracteres especiales.
'''
    longitud = raw_input('Introduzca la longitud de su password: ')
    password = string.ascii_lowercase + string.digits + string.ascii_uppercase + string.punctuation
    try:
        procesar = str(''.join(random.sample(password, int(longitud))))
        print Fore.GREEN + '[*]Su password es: ' + procesar
        passs()
    except:
        print Fore.RED + '[*]Reintentar. El valor maximo permitido es 94. No utilizar caracteres alfabeticos.'
        passs()
passs()

#Codigo escrito por Angelo Mass - Darko.

#La herramienta fue diseñada para el grupo
#"Auditoria global" y para la pagina
#"black hat"

#Hackea El Planeta.