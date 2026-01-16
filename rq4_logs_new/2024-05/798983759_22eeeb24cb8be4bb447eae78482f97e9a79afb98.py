'''
Una lista es una colección ordenada y mutable de elementos. 
Las listas pueden contener cualquier tipo de dato y pueden modificarse agregando, eliminando o modificando elementos.


append: agregar un elemento al final
remove: eliminar un elemento
actualizacion: nombrelista[indice] = valor

1   Crear menu de opciones
2   implementar ciclo para presentar menu de opciones
3   implementar funciones para cada opcion
4   Leer y escribir el contenido de la lista a un archivo
'''
#definir una lista vacia
frases = []

#Ingresar elementos
frases.append("La poesía de la tierra nunca ha muerto")
frases.append("La naturaleza no hace nada incompleto ni nada en vano")
frases.append("El buen hombre es el amigo de todos los seres vivos")
frases.append("Los árboles que tardan en crecer llevan la mejor fruta")
frases.append("La naturaleza sostiene la vida universal de todos los seres")

#Listar elementos de la lista
print("Lista de frases")
indice = 0
for frase in frases:
    print(f"{indice} {frase}")
    indice= indice+1
print("____________________________________________")    
print("Lista de frases usando función enumerate")
for indice,frase in enumerate(frases):
    print(f"{indice} {frase}")

#Actualizar elementos de la lista
frases[0] = "Creo que el futuro para la energía solar es brillante."
print("____________________________________________")    
print("Lista de frases usando función enumerate")
for indice,frase in enumerate(frases):
    print(f"{indice} {frase}")

#Eliminar elementos de la lista
frases.remove("La naturaleza sostiene la vida universal de todos los seres")
frases.pop(0)
frases.pop(0)
print("____________________________________________")    
print("Lista de frases usando función enumerate")
for indice,frase in enumerate(frases):
    print(f"{indice} {frase}")
