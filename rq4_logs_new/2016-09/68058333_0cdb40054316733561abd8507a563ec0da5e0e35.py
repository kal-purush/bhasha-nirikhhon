 '''
 Ejercicio 14.7.5. Botella y Sacacorchos
 =======================================

    a) Escribir una clase Corcho, que contenga un atributo bodega (cadena con
       el nombre de la bodega).

    b) Escribir una clase Botella que contenga un atributo corcho con una
       referencia al corcho que la tapa, o None si está destapada.

    c) Escribir una clase Sacacorchos que tenga un método destapar que le
       reciba una botella, le saque el corcho y se guarde una referencia al
       corcho sacado. Debe lanzar una excepción en el caso en que la botella
       ya esté destapada, o si el sacacorchos ya contiene un corcho.

    d) Agregar un método limpiar, que saque el corcho del sacacorchos, o lance
       una excepción en el caso en el que no haya un corcho.
 '''
class Corcho:
    def __init__(self, bodega):
        ''' Recibe el nombre de una bodega y lo almacena '''
        self.bodega = bodega

class Botella:
    def __init__(self, corcho=None):
        ''' Recibe un corcho y lo pone en la botella. Si está destapada,
            guarda `None` '''
        self.corcho = corcho

class Sacacorcho:
    def __init__(self):
        ''' Puede sacar los corchos de una botella '''
        self.corcho = None

    def destapar(self, botella):
        if self.corcho:
            raise ValueError("Ya tiene un corcho!")
        if botella.corcho:
            self.corcho = botella.corcho
            botella.corcho = None
        else:
            raise ValueError("La botella está destapada")

    def limpiar(self):
        if self.corcho:
            self.corcho = None
        else:
            raise ValueError("No hay corcho que sacar")
