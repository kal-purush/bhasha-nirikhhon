#descripcion: crear un programa que imprima una lista de divisores
#entrada: preguntar al usuario por un numero 
#salida: ejecutar como salida a la impresion de una lista con los divisores de ese numero
#autor: mvillalobos
#fecha:06/07/2017
#versin:2.0
#plataforma:python 2.7

num = raw_input("ingrese numero a dividir:")
for a in range(1,num):
 if num%a == 0:
  lista = []
    lista.append(a)
print(lista)