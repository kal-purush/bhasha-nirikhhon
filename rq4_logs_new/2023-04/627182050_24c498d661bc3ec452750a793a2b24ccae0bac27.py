
import json
from user import User, ExistingUserError

file_name = "users.json"


def mostrar_menu():
    print('')
    print('------------------------')
    print('MENU:')
    print('------------------------')
    print('1 - Crear usuario')
    print('2 - Iniciar sesion')
    print('3 - Salir')
    print('')

    opcion = input('Ingrese una opcion: ')

    if (opcion == '1'):
        crear_usuario()
    elif (opcion == '2'):
        iniciar_sesion()
    elif (opcion == '3'):
        exit()
    else:
        mostrar_menu()


def crear_usuario():
    print('')
    print('------------------------')
    print('CREAR USUARIO: ')
    print('------------------------')
    nombre_usuario = input('Introducir un nombre de usuario: ')
    contrasena_usuario = input('Introducir contraseña: ')
    repetir_contrasenia = input('Repetir contreseña: ')

    if (contrasena_usuario != repetir_contrasenia):
        print('ERROR: Las contraseñas no coinciden!!')
        crear_usuario()

    new_user = User(nombre_usuario, contrasena_usuario)

    try:
        new_user.create()
    except ExistingUserError:
        print('ERROR: Ya existe un usuario con ese nombre!! Intente con otro distinto.')
        crear_usuario()

    print('')
    print('USUARIO CREADO!!!')
    print('')

    mostrar_menu()


def iniciar_sesion():
    print('')
    print('------------------------')
    print('INICIAR SESION:')
    print('------------------------')
    name = input('Introducir un nombre de usuario: ')
    password = input('Introducir contraseña: ')

    user = User(name, password)
    user_login = user.login()

    if (user_login == None):
        print('')
        print('ERROR: Datos incorrectos.')
        print('')
        iniciar_sesion()
    else:
        print('')
        print('Usted ha iniciado sesion exitosamente!!')
        print('')


mostrar_menu()