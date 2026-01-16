from numpy import *

# arr = zeros((3,3))
#
# arr[0][2] = 2
#
# print arr
# print len(arr)

ciclo = True

mat1 = None
mat2 = None
valorMostrar = None
suma = None
def menu():
    print '''
    --Bienvenidos al creador de matrices--

    1)Crear una matriz nula(max: 2)
    2)Mostrar el contenido de una matriz
    3)Cambiar los valores de una matriz
    4)Cambiar las dimensiones de una matriz
    5)Sumar matrices (siempre y cuando tengan las mismas dimensiones)
    6)Mostrar resultado de la suma de matrices
    7)salir
    '''
def mostrar():
    global valorMostrar
    print '''Puedes elegir:
    1)Matriz 1
    2)Matriz 2
    '''
    opc = int(raw_input(' Que matriz desea escoger?: '))

    if opc == 1:
        valorMostrar = 1
    elif opc == 2:
        valorMostrar = 2
def cambiarValores(matriz):
    for j in range(len(matriz)):
        for k in range(len(matriz[j])):
            valor = '\n Usted se encuentra en la posicion {0}, {1} que valor desea: '.format((j+1),(k+1))
            matriz[j][k]= float(raw_input(valor))
def crearMatriz():
    global mat1
    global mat2
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
    global mat1
    global mat2
    if mat1 is not None and mat2 is not None:
        mostrar()
        if valorMostrar == 1:
            print mat1
        elif valorMostrar == 2:
            print mat2
    elif mat1 is None:
        if mat2 is None:
            print '\n No existe ninguna matriz'
        elif mat2 is not None:
            print '\n %s'%(mat2)
    elif mat1 is not None:
        print '\n %s'%(mat1)
def llenarMatriz():
    global mat1
    global mat2
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
        global mat1
        global mat2
        elegir = raw_input('\n Esta funcion borrara la matriz elegida y la llenara de zeros\n Desea seguir(s/n): ')
        if elegir == 's':
            mostrar()
            if valorMostrar == 1:
                mat1 = None
                print mat1
                filas = int(raw_input('\n Cuantas filas desea que tenga la matriz: '))
                col = int(raw_input(' Cuantas columnas desea que tenga la matriz: '))
                mat1 = zeros((filas,col))
                print '\n ',mat1
            elif valorMostrar == 2:
                mat2 = None
                print mat2
                filas = int(raw_input('\n Cuantas filas desea que tenga la matriz: '))
                col = int(raw_input(' Cuantas columnas desea que tenga la matriz: '))
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
def sumarMatriz():
    global mat1
    global mat2
    global suma
    for j in range(len(mat1)):#determino cuantas filas contiene la matriz
        for k in range(len(mat1[j])):#determino cuantas columnas contiene la matriz
            mat1Col = len(mat1[j])
        mat1Fil = len(mat1)
    for j in range(len(mat2)):#determino cuantas filas contiene la matriz
        for k in range(len(mat2[j])):#determino cuantas columnas contiene la matriz
            mat2Col = len(mat2[j])
        mat2Fil = len(mat2)

    if mat1Fil == mat2Fil:
        if mat1Col == mat2Col:
            suma = mat1 + mat2
    else:
        print '''
        No es posible sumar matrices de diferentes dimensiones:
        matriz 1 = {0} filas y {1} columnas mientras que
        matriz 2 = {2} filas y {3} columnas.
        '''.format(mat1Fil,mat1Col,mat2Fil,mat2Col)
while ciclo == True:
    menu()
    opc = int(raw_input(' Eliga una opcion porfavor: '))
    if opc == 7:
        ciclo = False
    elif opc == 1:
        crearMatriz()
    elif opc == 2:
        mostrarMatriz()
    elif opc == 3:
        llenarMatriz()
    elif opc == 4:
        cambiarDimension()
    elif opc == 5:
        sumarMatriz()
    elif opc == 6:
        if suma is None:
            print '\n No ha realizado ninguna suma todavia'
        elif suma is not None:
            print suma