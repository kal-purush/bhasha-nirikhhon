# Funciones que permiten realizar validaciones

def validar_longitud_datos_registro(nombre, correo, nombre_usuario, contrasenia):
    
    if nombre == '' or len(nombre) < 3:
        return False, {'mensaje': 'El campo nombre no puede estar vacio o tener menos de 3 caracteres.'}

    if correo == '' or len(correo) == 0:
        return False, {'mensaje': 'El campo correo no puede estar vacio.'}
    
    if nombre_usuario == '' or len(nombre_usuario) == 0:
        return False, {'mensaje': 'El campo nombre de usuario no puede estar vacio.'}
    
    if contrasenia == '' or len(contrasenia) == 0:
        return False, {'mensaje': 'El campo contrasenia no puede estar vacio.'}
    
    return True, {'mensaje': 'No hay errores de longitud'}



def validar_correo():
    pass



def validar_nombre_usuario_en_db():
    pass



def validar_corre_en_db():
    pass

