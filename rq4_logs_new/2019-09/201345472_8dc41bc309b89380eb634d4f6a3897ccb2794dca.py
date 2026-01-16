#Cargar una lista con 5 valores positivos, al finalizar mostrar la lista

list = [1, 2, 3, 4, 5]

i = 0

for i in range(5):
    print(list[i])

list2 = []

list2.append(1)
list2.append(2)
list2.append(3)
list2.append(4)
list2.append(5)

for i in range(5):
    print(list2[i])

print(list2)

#Cargar una lista con 6 valores numericos positivos, por fin de carga recorrerla e informar en que posicion esta el mayor

list3 = []

list3.append(5)
list3.append(6)
list3.append(23)
list3.append(45)
list3.append(9)

mayorValor = list3[0]
posicion = 0


for i in range(5):
    if list3[i] > mayorValor:
        mayorValor = list3[i]
        posicion = i

print("El elemento mas grande se encuentra en la posicion: ", posicion, "y tiene el valor ", mayorValor)