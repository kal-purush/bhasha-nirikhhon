#!/usr/bin/env python
# v1.5.2
import sys
import os
import glob
from lib.libbvf import *

def main():
    ## Cambio en la condición inicial
    if (len(sys.argv) < 2 or len(sys.argv) > 6):
        print("VotoApp-cli v0.2\nUso: VotoApp-cli [-g] ARCHIVO_RSA ARCHIVO_VOTO [Voto] [Margen] \n\
Opciones:\n    -g:              Generar archivo de voto.\n\
Parámetros:\n\
    ARCHIVO_RSA:     Clave pública o privada para encriptar o desencriptar, respectivamente.\n\
    ARCHIVO_VOTO:    Archivo de destino para guardar o leer el voto emitido. Si se quiere desencriptar, puede usarse igualmente un directorio completo.\n\
    [Voto]:          Opción que se desea plasmar en el voto.\n\
    [Margen]:        Texto al margen, usado para anotar cosas sin anular el voto. Si no desea añadir nada, use un string vacío o un espacio.")
    else:
        ## Cambio en la asignación del voto
        if (sys.argv[1] == '-g'):
            mivoto = Voto()
            ## Si no hay argumentos con el voto (y margen), se solicita
            if (len(sys.argv) <= 4):
                main_text = input("\nIngresa el texto de tu voto: ")
                mivoto.main = main_text+' '
                free_text = input("Ingresa el texto del margen: ")
                mivoto.free = free_text+' '
            ## Si sólo está el voto, se pregunta por si se desea agregar margen
            elif(len(sys.argv) == 5):
                mivoto.main = sys.argv[4]+' '
                print("\nVoto recibido!")
                free_text = input("Si lo deseas, ingresa un texto al margen: ")
                mivoto.free = free_text+' '
            ## Si se agregaron voto y margen, se omite interacción con el usuario
            else:
                mivoto.main = sys.argv[4]+' '
                mivoto.free = sys.argv[5]+' '
            mivoto.free = free_text+' '
            mivoto.setKey()
            mivoto.encryptVote()
            writeToFile(sys.argv[3], sys.argv[2], mivoto)
            print("Voto generado:")
            ## Si se agregó margen, se indica
            if(len(mivoto.free) > 1):
                print("Votaste por la opción '"+mivoto.main.strip()+"' y anotaste al margen '"+mivoto.free.strip()+"'.")
            ## Si no se agregó margen, se deja explícitamente indicado que no fue agregado por opción personal
            else:
                print("Votaste por la opción '"+mivoto.main.strip()+"' y no hiciste anotación al margen.")
        else:
            if (os.path.isdir(sys.argv[2])):
                for filename in glob.glob(os.path.join(sys.argv[2], '*.bvf')):
                    mivoto = readFromFile(filename, sys.argv[1])
                    with open(os.path.splitext(filename)[0]+'.json', 'w') as f:
                        f.write(mivoto.toJson())
                    print(mivoto.toJson())
            else:
                mivoto = readFromFile(sys.argv[2], sys.argv[1])
                print(mivoto.toJson())
    sys.exit(0)

if __name__ == '__main__':
    main()