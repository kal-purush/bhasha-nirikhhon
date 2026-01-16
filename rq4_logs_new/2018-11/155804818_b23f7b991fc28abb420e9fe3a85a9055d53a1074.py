import csv

def chequeoerror():
	##### AGREADO POR PEKE
	try:
		with open('ventas.csv','r') as archivo:
			tablacsv = list(csv.DictReader(archivo))
			tablaorden = ["CODIGO","PRODUCTO","CLIENTE","PRECIO","CANTIDAD"]
			lista = []

			for i in tablacsv:
				for a in tablaorden:
					if i[a] == "" or str(i[a]).isspace():
						lista.append(f"Al registro numero {tablacsv.index(i)} le falta el campo {a}.")

			for i in range(len(tablacsv)):
				if tablacsv[i]["CODIGO"][0:3].isalpha() and tablacsv[i]["CODIGO"][3:6].isnumeric() and len(tablacsv[i]["CODIGO"]) == 6:
					continue
				else:
				   lista.append(f"El Codigo del registro numero {i} tiene un formato invalido.")

			for i in range(len(tablacsv)):
				if str(tablacsv[i]["CANTIDAD"]).isspace() or tablacsv[i]["CANTIDAD"] == "":
					lista.append(f"Al registro numero {i} le falta el campo CANTIDAD.")
					break
				elif float(tablacsv[i]["CANTIDAD"]) % 1 != 0:
					lista.append(f"El campo CANTIDAD del registro numero {i} no es un numero entero.")

				if float(tablacsv[i]["PRECIO"]):
					continue
				else:
					lista.append(f"El campo PRECIO del registro numero {i} no es un numero decimal.")

			lista = list(set(lista))
			return lista

	except FileNotFoundError:
		return "Archivo no encontrado."


def ultventfun():
	with open('ventas.csv','r') as archivo:
		tablacsv = csv.DictReader(archivo)
		tablaorden = ["CODIGO","PRODUCTO","CLIENTE","PRECIO","CANTIDAD"]
		tabla = []
		tablafinal = []
		for line in tablacsv:
			tabla.append(line)

		for fila in range(len(tabla)):  #SE REVISA LA CANTIDAD DE VALORES QUE TIENE LA LISTA TABLA
			tablafinal.append([])   #SE AGREGA UNA LISTA VACIA A LA LISTA TABLAFINAL
			for columna in range(len(tablaorden)):  #SE REVISA LA CANTIDAD DE ELEMENTOS EN TABLAORDEN
				tablafinal[fila].append(tabla[fila][tablaorden[columna]])   #SE GUARDA EL VALOR DE LA TABLA EN EL ORDEN DE TABLAORDEN

		x = tablaorden.index("PRECIO")
		for i in range(len(tablafinal)):
			tablafinal[i][x] = "$"+"%.2f" % float(tablafinal[i][x])

		return (tablafinal,tablaorden)

def filtrar(campo, filtro):
	with open('ventas.csv','r') as archivo:
		tablacsv = csv.DictReader(archivo)
		listado = []

		for linea in tablacsv:
			if filtro.lower() in linea[campo].lower():
				listado.append(linea[campo])

		listado = list(set(listado))

		return listado

def mostrar(campo, filtro):
	with open('ventas.csv','r') as archivo:
		tablacsv = csv.DictReader(archivo)

		cant = 0
		tabla = []
		tablafinal = []
		tablaorden = ["CODIGO","PRODUCTO","CLIENTE","PRECIO","CANTIDAD"]
		nombre = filtro.replace("%20", " ")

		for line in tablacsv:
			if line[campo] == nombre:
				tabla.append(line)

		for fila in range(len(tabla)):
			tablafinal.append([])
			for columna in range(len(tablaorden)):
				tablafinal[fila].append(tabla[fila][tablaorden[columna]])

		x = tablaorden.index("PRECIO")
		for i in range(len(tablafinal)):
			tablafinal[i][x] = "$"+ "%.2f" % float(tablafinal[i][x])

		if campo == "PRODUCTO":
			for cantidad in range(len(tabla)):
				cant = cant + int(tabla[cantidad]["CANTIDAD"])
		elif campo == "CLIENTE":
			for precio in range(len(tabla)):
				cant = cant + (float(tabla[precio]["PRECIO"])*int(tabla[precio]["CANTIDAD"]))

		cant = "%.2f" % cant
		return tablafinal, tablaorden, cant, nombre

def mejores(campo):
	with open('ventas.csv','r') as archivo:
		tablacsv = list(csv.DictReader(archivo))

		tabla = []
		diccionario = {}

		for nombre in tablacsv:     #ESTE FOR RECORRE TODA LA TABLA Y GUARDA LOS NOMBRES
			tabla.append(nombre[campo])
		tabla = list(set(tabla))       #DESECHA LOS DUPLICADOS, QUEDANDO SOLO 1 NOMBRE DE CADA UNO QUE APARECE

		for compra in range(len(tabla)):    #SE REVISA LA TABLA DE LOS NOMBRES SOLAMENTE
			valor = 0
			for linea in range(len(tablacsv)):  #SE REVISA LA TABLA GENERAL, EL ARCHIVO
				if tablacsv[linea][campo] == tabla[compra]: #SI EL VALOR DE LA TABLA GENERAL ES IGUAL AL NOMBRE DE LA LISTA
					if campo == "CLIENTE":
						valor = valor + (float(tablacsv[linea]["PRECIO"]) * int(tablacsv[linea]["CANTIDAD"]))   #GUARDA EL VALOR DE ESE REGISTRO DE LA TABLA, HACIENDO PRECIO*CANTIDAD
					elif campo == "PRODUCTO":
						valor = valor + int(tablacsv[linea]["CANTIDAD"])    #SI LO QUE QUERES ES SOLO GUARDA LA CANTIDAD DE LOS PRODUCTOS GUARDA LA CANTIDAD
			diccionario[tabla[compra]] = ("$"+ "%.2f" % valor if campo == "CLIENTE" else valor) #SE INGRESA EL NOMBRE DE LA LISTA Y EL RESULTADO DE LA CUENTA
		
		ordenado = sorted(diccionario.items(), key = lambda t:t[1], reverse=True)
		return ordenado

##### AGREADO POR PEKE