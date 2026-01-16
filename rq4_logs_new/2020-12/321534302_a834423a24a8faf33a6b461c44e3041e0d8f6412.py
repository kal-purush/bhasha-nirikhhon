# constantes y variables
masa = 34
gravedad = 9.8
peso = masa * gravedad
if masa / 2 == 0 or masa >= 0:
    print('El valor del peso es: ' + str(peso))
else:
    print('la masa no es valida')

# listas, tuplas y diccionarios
lista = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(lista, type(lista))
print(lista[2], type(lista[2]))

# cambiamos un valor de una lista
lista[1] = 22
print(lista)

# agregamos mas valores a la lista
lista.append(11)
print(lista)

# lista de lista
lista2 = [1, 2, 3, 4]
lista3 = [5, 6, 7, 8]
listaMaestra = [lista2, lista3]
print(listaMaestra[1], type(listaMaestra))

# tuplas estas no se pueden modificar
tupla = (1, 2, 3, 4, 5, 6, 7)
print(tupla, type(tupla))

# convertimos una tupla a una lista
frutas = ('manzana', 'melon', 'guineo')
milista = list(frutas)
print(frutas, type(frutas))
print(milista, type(milista))

# diccionarios
dic = {
    'objeto': 'pelota',
    'color': 'azul',
    'diametro': 20
}
print(dic)