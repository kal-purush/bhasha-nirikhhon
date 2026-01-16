from numpy import *

# arr = zeros((3,3))
#
# arr[0][2] = 2
#
# print arr
# print len(arr)

ciclo = True

mat1 = zeros((2,2))
mat2 = zeros((3,3))
valorMostrar = None
def menu():
    print '''
    --Bienvenidos al creador de matrices--

    1)Crear una matriz nula(max: 2)
    2)Mostrar el contenido de una matriz
    3)Cambiar los valores de una matriz
    4)Cambiar las dimensiones de una matriz
    5)Sumar matrices(siempre y cuando tengan las mismas dimensiones)
    6)salir
    '''
def mostrar():
    global valorMostrar
    print '''Puedes elegir:
    1)Matriz 1
    2)Matriz 2
    '''
    opc = int(raw_input('Que matriz desea ver?: '))

    if opc == 1:
        valorMostrar = 1
    elif opc == 2:
        valorMostrar = 2
def cambiarValores(matriz):
    for j in range(len(matriz)):
        for k in range(len(matriz[j])):
            valor = 'Usted se encuentra en la posicion {0}, {1} que valor desea: '.format((j+1),(k+1))
            matriz[j][k]= float(raw_input(valor))
def crearMatriz():
    if mat1 is not None:
        if mat2 is not None:
            print '\n Ya alcanzo el maximo de matrices que puede crear que es 2'
        elif mat2 is None:
            filas = int(raw_input('\n Cuantas filas quieres que tenga la matriz: '))
            col = int(raw_input(' Cuantas columnas quieres que tenga la matriz: '))
            mat2 = zeros((filas,col))
            print '\n Matriz creada exitosamente'
    elif mat1 is None:
        filas = int(raw_input('\n Cuantas filas quieres que tenga la matriz: '))
        col = int(raw_input(' Cuantas columnas quieres que tenga la matriz: '))
        mat1 = zeros((filas,col))
        print "\n Matriz creada exitosamente"
def mostrarMatriz():
    if mat1 is not None and mat2 is not None:
        mostrar()
        if valorMostrar == 1:
            print mat1
        elif valorMostrar == 2:
            print mat2
    elif mat1 is None:
        if mat2 is None:
            print ' No existe ninguna matriz'
        elif mat2 is not None:
            print '\n%s'%(mat2)
    elif mat1 is not None:
        print '\n%s'%(mat1)
def llenarMatriz():
    if mat1 is None and mat2 is None:
        print '\n No existe ninguna matriz'
    elif mat1 is not None and mat2 is not None:
        mostrar()
        if valorMostrar == 1:
            cambiarValores(mat1)
        elif valorMostrar == 2:
            cambiarValores(mat2)
    elif mat2 is not None:
        cambiarValores(mat2)
    elif mat1 is not None:
        cambiarValores(mat1)
def cambiarDimension():
    seg = False
    def seguro():
        elegir = raw_input('\n Esta funcion borrara la matriz elegida y la llenara de zeros\n Quieres seguir(s/n): ')
        if elegir == 's':
            mostrar()
            if valorMostrar == 1:
                mat1 = None
                print mat1
                filas = int(raw_input('\n Cuantas filas quieres que tenga la matriz: '))
                col = int(raw_input(' Cuantas columnas quieres que tenga la matriz: '))
                mat1 = zeros((filas,col))
                print '\n ',mat1
            elif valorMostrar == 2:
                mat2 = None
                print mat2
                filas = int(raw_input('\n Cuantas filas quieres que tenga la matriz: '))
                col = int(raw_input(' Cuantas columnas quieres que tenga la matriz: '))
                mat2 = zeros((filas,col))
                print '\n ',mat1
        elif elegir == 'n':
            pass

    if mat1 is not None:
        if mat2 is not None:
            seguro()
        elif mat2 is None:
            print '\n Antes de usar esta funcion necesita tener creada el maximo de matrices'
    elif mat1 is None:
        print '\n Antes de usar esta funcion necesita tener creada el maximo de matrices'

while ciclo == True:
    menu()
    opc = int(raw_input(' Eliga una opcion porfavor: '))
    if opc == 6:
        ciclo = False
    elif opc == 1:
        crearMatriz()
    elif opc == 2:
        mostrarMatriz()
    elif opc == 3:
        llenarMatriz()
    elif opc == 4:
        cambiarDimension()