#Quiero ir al super a comprar ingredientes que me faltan para una pizza
#Necesito imprimir una lista de los ingredientes para guiarme en el super
#Voy a hacerlo a partir de una FUNCION porque esto me permitiria volver a utilizar la formula
#cada vez que necesite imprimir una lista segun mi necesidad, puedo volver a usar esta funcion
#para obtener el mismo resultado con diferentes valores 

ingre_pan = ["harina", "levadura", "sal", "azucar", "agua"] #lista de ingredientes que quiero que se imprima

def imprimir_lista(ingredientes, nombre_producto): #creo la funcion para imprimir la lista con los parametros(que serian las variables )

    print("Lista de compras para", nombre_producto)
    print("-------------------")

    for producto in ingredientes:
        print(producto)
imprimir_lista(ingre_pan,"pan")

ingre_salsa = ["tomate", "azucar", "sal", "cebolla"]

imprimir_lista(ingre_salsa,"Salsa")