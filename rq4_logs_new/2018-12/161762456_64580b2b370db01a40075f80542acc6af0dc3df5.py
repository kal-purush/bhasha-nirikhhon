"""

Desarrollador/es:   Francisco Maurino   77629
                    Facundo Mentesana   75387

Cátedra: Algoritmos y Estructura de Datos

Curso:  1k03
Año:    2017

-------------------------------------------------

ALGUNAS CONSIDERACIONES:

El siguiente codigo fue programado para ser ejecutado en la consola de sistema operativo
ya sea Command Prompt (Windows) o en la Terminal (Linux), ya que se usan modulos de sistema
operativo para limpiar pantalla y un modulo llamado Colorama, el cual se encarga de
darle color (rojo o negro) a los numeros del dibujo de la mesa para hacer el programa mas
agradable al usuario.
Si se utiliza la consola de PyCharm o algun otro IDE, probablemente no muestre los colores
de los numeros del dibujo de la mesa.

Para saber mas acerca del modulo colorama visitar:

https://pypi.python.org/pypi/colorama

La funcion para limpiar pantalla se encuentra seteada para WINDOWS POR DEFECTO,
por ende, si usted esta utilizando Linux debe comentar la linea 79 y habilitar
la linea 80.

"""

# ---------------------------------------------------------------------------------- MODULOS
import os
import random

# Funciones a utilizar del modulo Colorama
from colorama import Back
from colorama import Fore
from colorama import Style
from colorama import init
# Funcion de inicializacion de modulo Colorama
init()

# ---------------------------------------------------------------------------------- DECLARACION DE VARIABLES
print('¡BIENVENIDO AL JUEGO "RULETA AED"!'
      '\n\nUsted dispone de 6 fichas equivalentes a $600.\nVamos a apostar...'
      '(Ingrese 1, 2 ó 3 segun corresponda)\n')

fichas = 6
fichas_ganadas = 0
fichas_perdidas = 0

secuencia = ('rojo', 'negro', 'par', 'impar', 'falta', 'pasa')

menu = int(input("1 -> Rojo\n2 -> Negro\nEleccion: "))
color = secuencia[menu-1]
menu = int(input("\n1 -> Par\n2 -> Impar\nEleccion: "))
paridad = secuencia[menu+1]
menu = int(input("\n1 -> Falta\n2 -> Pasa\nEleccion: "))
pasafalta = secuencia[menu+3]
docena = int(input('\n¿Prefiere la docena "1", "2" o "3"?: '))
columna = int(input('\n¿Prefiere la columna "1", "2" o "3"?: '))
pleno = int(input('\nElija un numero del 0 al 36 para apostar pleno: '))


# DECLARACION DE FLAGS
num_rojos = (1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36)
color_f = ''
paridad_f = ''
pasafalta_f = ''
docena_f = 0
columna_f = 0
pleno_f = False

# ---------------------------------------------------------------------------------- GENERACION NUMERO ALEATORIO
num = random.randint(0, 36)

os.system('cls')        # Parar Windows
# os.system('clear')    # Para Linux

print('\nUsted aposto a:', color, ',', paridad, ',', pasafalta, ', docena:',
      docena, ', columna:', columna, ', pleno:', pleno,
      '\n\nGIRANDO ... GIRANDO ... GIRANDO ...\nHA SALIDO EL', num)


# ---------------------------------------------------------------------------------- CHECK CERO
if num:
    # -------------------------------------------- CHECK FLAG COLOR
    if num in num_rojos:
        color_f = 'rojo'
    else:
        color_f = 'negro'

    # -------------------------------------------- CHECK FLAG DE PARIDAD
    if num % 2:
        paridad_f = 'impar'
    else:
        paridad_f = 'par'

    # -------------------------------------------- CHECK FLAG DE PASA/FALTA
    if 1 <= num <= 18:
        pasafalta_f = 'falta'
    else:
        pasafalta_f = 'pasa'

    # -------------------------------------------- CHECK FLAG DE DOCENA
    if 1 <= num <= 12:
        docena_f = 1
    elif 13 <= num <= 24:
        docena_f = 2
    elif 25 <= num <= 36:
        docena_f = 3

    # -------------------------------------------- CHECK FLAG DE COLUMNA
    if num % 3 == 1:
        columna_f = 1
    elif num % 3 == 2:
        columna_f = 2
    elif num % 3 == 0:
        columna_f = 3

if num == pleno:
    pleno_f = True

# ---------------------------------------------------------------------------------- CHECK DE FICHAS
# -------------------------------------------- CHECK PARIDAD
if paridad == paridad_f:
    fichas_ganadas += 1
else:
    fichas_perdidas += 1

# -------------------------------------------- CHECK COLOR
if color == color_f:
    fichas_ganadas += 1
else:
    fichas_perdidas += 1

# -------------------------------------------- CHECK PASA/FALTA
if pasafalta == pasafalta_f:
    fichas_ganadas += 1
else:
    fichas_perdidas += 1

# -------------------------------------------- CHECK DOCENA
if docena == docena_f:
    fichas_ganadas += 2
else:
    fichas_perdidas += 1

# -------------------------------------------- CHECK COLUMNA
if columna == columna_f:
    fichas_ganadas += 2
else:
    fichas_perdidas += 1

# -------------------------------------------- CHECK PLENO
if pleno_f:
    fichas_ganadas += 35
else:
    fichas_perdidas += 1


# -------------------------------------------- GANADAS/PERDIDAS
fichas += fichas_ganadas
fichas -= fichas_perdidas


# ---------------------------------------------------------------------------------- CALCULO DE GANANCIAS
ganancia = (fichas * 100)
comision = (5*ganancia)/100
ganancia -= comision

if fichas_ganadas:
    print('\n¡Usted ha ganado', fichas_ganadas, 'fichas!')
    if fichas_perdidas:
        print('Pero ha perdido', fichas_perdidas, '...')
else:
    print('\nNo ha ganado fichas.')
    if fichas_perdidas:
        print('Usted ha perdido', fichas_perdidas, 'fichas... ')

print('\nDispone de un total de', fichas, 'fichas'
      ' equivalentes a $', ganancia,
      '(teniendo en cuenta una comisión del casino de: $'+str(comision)+')\n\n')


# ---------------------------------------------------------------------------------- DIBUJO DE RULETA CON COLORES
#                                                                                    DE MODULO COLORAMA

print('\t*-----------------------------------*\n\t|'
      + Back.WHITE + Fore.RED + '  3' + Fore.BLACK + ' 6' + Fore.RED + ' 9' ' 12' + Fore.BLACK +
      ' 15' + Fore.RED + ' 18' ' 21' + Fore.BLACK + ' 24' + Fore.RED+' 27' ' 30' + Fore.BLACK +
      ' 33' + Fore.RED + ' 36' + Style.RESET_ALL + ' | --> Columna 3\n\t|'+Back.WHITE+Fore.GREEN +
      '0 '+Fore.BLACK+'2' + Fore.RED + ' 5' + Fore.BLACK + ' 8' ' 11' + Fore.RED + ' 14' + Fore.BLACK +
      ' 17' ' 20' + Fore.RED + ' 23' + Fore.BLACK + ' 26' ' 29' + Fore.RED + ' 32' + Fore.BLACK + ' 35'
      + Style.RESET_ALL + ' | --> Columna 2\n\t|' + Back.WHITE +
      '  ' + Fore.RED + '1' + Fore.BLACK + ' 4' + Fore.RED + ' 7' ' 10' + Fore.BLACK +
      ' 13' + Fore.RED + ' 16' ' 19' + Fore.BLACK + ' 22' + Fore.RED + ' 25' ' 28' + Fore.BLACK +
      ' 31' + Fore.RED + ' 34' + Style.RESET_ALL + ' | --> Columna 1\n'
      '\t*-----------------------------------*\n'
      '\t  |  DOC 1 |   DOC 2   |    DOC 3   |\n'
      '\t  |     FALTA    |       PASA       |')


input("\n\n¡GRACIAS POR JUGAR!\n\nPresione ENTER para salir...")