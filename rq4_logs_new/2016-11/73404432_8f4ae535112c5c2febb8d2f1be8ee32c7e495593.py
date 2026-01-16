## UTILIDADES DE DEPURACION ##

def checkSeleccionaCancionRandom(cancion, libreria):


def checkPlayShuffle(playList):

## RUTINAS DE UTILIDADES ##
import random
import os
import shlex
def seleccionaCancionRandom(libreria):

assert isinstance(libreria, dict)
 #chekear las estructuras de datos para saber que no se han corrompido, se llaman invariantes
    listacanciones = list(libreria.keys())
    titulocancion = random.choice(listacanciones)
    return titulocancion

# return titulo y location

def iniciarPlayList(titulocancion):

    assert isinstance(playlist, dict), "No es un diccionario!"
    assert titulocancion in libreria, "No esta en la playlist!"
    assert # la playlist tiene que ser un string##

    # seleccionar cancion // comprobar que esta en la playlist // si esta la descartamos
    # y llamamos a seleccionaCancionRandom // Si no esta añadimos
    cancionesIntroducidas = 0
    while cancionesIntroducidas < len(libreria):
        cancionAIntroducir = seleccionaCancionRandom(libreria)
        if cancionAIntroducir not in playList:
            valor = "valor"
            playlist[valor1:] = seleccionaCancionRandom(libreria)
            valor = "valor"+1

             cancionesIntroducidas += 1


def imprimirCancionesReproducidas(playList):

# 1: Cancion1
# 2: Cancion2
# 3: Cancion3

def lanzarVLC(libreria, playList):

# libreria = conjunto de canciones
# playlist = numero cancion

    import subprocess
    import shlex
    import os

    linuxPathVLC = "/usr/bin/vlc" # Poner ruta en Win
    lineaComandoVLC = linuxPathVLC
    separador = " "

    for numeroCancion in sorted(playList.keys()):
        tituloCancion = playList[numeroCancion]
        try:
            rutaAccesoFichero = libreria[tituloCancion]["location"]
        except KeyError:
            print("la cancion " + str(tituloCancion) + " no se encuentra en la biblioteca")
        else:
            if os.path.exists(str(rutaAccesoFichero)):
                lineaComandoVLC = lineaComandoVLC + separador + str(rutaAccesoFichero)
            else:
                pass

    # Popen necesita una lista de string
    args = shlex.split(lineaComandoVLC)

    try:
        procesoVLC = subprocess.Popen(args)
        # procesoVLC = subprocess.Popen(["/usr/bin/vlc", "California_Uber_Alles.mp3", "Seattle_Party.flac"])
    except OSError:
        print("el fichero no existe")
    except ValueError:
        print("argumentos invalidos")
    else:
        print("lanzando VLC con lista aleatoria")


## FUNCION PRINCIPAL ##


def playSuffle(libreria, playList):


# Genera la lista de canciones aleatorias

## PROGRAMA PRINCIPAL ##

playList = {}


libreria = {"California_Uber_Alles.mp3":
                {"track-number": 3, "artist": "Dead Kennedys", "album": "Dead Kennedys", "location": "./biblioteca/California_Uber_Alles.mp3"},
            "Seattle_Party":
                {"track-number": 1, "artist": "Chastity Belt", "album": "No regrets", "location": "./biblioteca/Seattle_Party.flac"},
            "King_Kunta":
                {"track-number": 3, "artist": "Kendrick Lamar", "album": "To Pimp A Butterfly", "location": "./biblioteca/King_Kunta.mp3"}
            }

# for i in range(1,1001):
assert playSuffle(libreria, playList)

# libreriaLista = []
# assert playSuffle(libreriaLista)

imprimirCancionesReproducidas(playList)

lanzarVLC(libreria, playList)