# Declaraciones "if", "Elif" y "else"

hambre = True
sed = True

if (hambre):
    print("Tenemos hambre!")

elif(hambre and sed):
    print("Tenemos hambre y sed")

else:
    print("Estamos satisfechos")



# Ciclos For


milista = [1,2,3,4,5,6,7,8,9,10]

for num in milista:    # Para "num" se puede poner cualquier palabra
    print(num)

for num in milista:
    print("hola")      # se imprimira 10 veces "hola"


for jamon in milista:
    if jamon % 2 == 0:
        print(jamon)



suma_lista = 0

for num in milista:
    suma_lista = suma_lista + num
    print(suma_lista)


for letter in "Hola Mundo":     #Para imprimir caracter por caracter
    print(letter)


milista2 = [(1,2),(3,4),(5,6),(7,8)]    #pares de tuplas en una lista

for item in milista2:
    print(item)


for (a,b) in milista2:      # asi se desgloza uno por uno los numeros
    print(a)
    print(b)


d = {'y1':1,'y2':2,'y3':3}   # para imprimir diccionarios y sus valores

for item in d.items():
    print(item)

for llave,valor in d.items():
    print(valor)

e = {'y1':3,'y2':1,'y3':2} 

print("\n")
for value in d.values():     # para imprimir los valores en desorden, los diccionarios no tienen orden
    print(value)



# Ciclo  While
# El while tambien se puede combinar con un "else" despues


x = 0

while x <= 5:
    print("El valor actual de x es", x)
    x = x + 1  # o decir "x += 1"
else:
    print("El ciclo for termino con exito")



#Ahora se veran:
#  break
#  continue
#  pass

x1 = "Alexander"

for letter in x1:
    if letter == 'x':
        continue            # En si solo se salta ese caracter marcado
    print(letter)

print("\n")

for letter in x1:
    if letter == 'x':
        break           # Aqui rompe al ciclo al llegar a el caracter marcado
    print(letter)




##  Operadores utiles



milista3 = [1,2,3]
print("\n")
for num in range(10):        #sirve para un rango esta funcion del 0 al 9
    print(num)

print("\n")
for num in range(0,15,2):     # del cero al 15 en pasos de "2"
    print(num)


print(list(range(0,11,2)))       # para una lista como usar la funcion "range"



# una funcion importante de python es la siguiente
contador_indice = 0
palabra = "hola"

for letter in palabra:
    print(palabra[contador_indice])
    contador_indice += 1


for item in enumerate(palabra):   #para mostrar la letra y su numero de iteracion, se muestra en pares
    print(item)


for index,letter in enumerate(palabra):
    print(index)
    print(letter)
    print("\n")

milista_a = [1,2,3]
milista_b = ['a','b','c']

for item in zip(milista_a,milista_b):     #la funcion "zip()" se utiliza para emparejar elementos de las tistas a comparar
    print(item)


if 'a' in milista_b:       #aqui se imprimira verdadero si en la lista_a se encuentra el caracter 'a
    print("Verdadero")
else:
    print("Falso")

print(min(milista_a))      # imprime el minimo de la lista


print(max(milista_a))      #imprimir el valor maximo de una lista


from random import shuffle  # para importar

# Para Python 3
resultado = input("Escribe un numero aqui ")
print(resultado)





# Comprension de listas
#como agregar una cadena a una lista con append()

mi_cadena = "hola"

mi_lista = []

for letter in mi_cadena:
    mi_lista.append(letter)

print(mi_lista)


mi_lista = [x for x in range(0,11)]    # Se crea una lista de manera rapida
print(mi_lista)


mi_lista = [x**2 for x in range(0,11)]    # Se eleva al cuadrado el numero del 0 al 10
print(mi_lista)


#Para realizar una conversion de celsius a fahrenheit y por medio de un ciclo

Celsius = [0, 4.3, 56.3, 23, -14]

Fahrenheit = [((9/5)*temp + 32) for temp in Celsius]
print(Fahrenheit)