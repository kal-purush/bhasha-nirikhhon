"""
Programa estacion.py
Autor: Abel García

Dada una fecha como mes y día, indicar a qué estación corresponde.

1. Constantes:

Estación        Fecha de inicio
Primavera       20 de marzo
Verano          21 de junio
Otoño           22 de septiembre
Invierno        21 de diciembre
    
2. Los datos de entrada son:
    día
    mes

3. Cálculos:
    a) Para saber la estación que corresponde a la fecha, se debe comparar con los rangos de fechas

4. Los datos de salida son:
    La estación.
"""

# Se piden los datos de entrada
print()
dia = int(input("Día: "))
mes = input("Mes: ")


# Se decide qué tipo de triángulo es
if mes == "enero" or mes == "febrero":
    estacion = "invierno"
elif mes == "marzo":
    if dia <= 20:
        estacion = "invierno"
    else:
        estacion = "primavera"
elif mes == "abril" or mes == "mayo":
    estacion = "primavera"
elif mes == "junio":
    if dia <= 21:
        estacion = "primavera"
    else:
        estacion = "verano"
elif mes == "julio" or mes == "agosto":
    estacion = "verano"
elif mes == "septiembre":
    if dia <= 22:
        estacion = "verano"
    else:
        estacion = "otoño"
elif mes == "octubre" or mes == "noviembre":
    estacion = "otoño"
elif:
    if dia <= 21:
        estacion = "otoño"
    else:
        estacion = "invierno"


# Se despliega el resultado
print("La estación es", estación)
print()