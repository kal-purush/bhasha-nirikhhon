from Estequiometria.Conversiones import Conversiones
from Estequiometria.Masa import Masa
from Estequiometria.Moles import Moles
from Estequiometria.Solucion import  Solucion
from Estequiometria.Liquido import Liquido
from Estequiometria.Gas import Gas
from Estequiometria.problemaEsteq import problemaEsteq
from Reaccion import Reaccion

def menu(opciones, texto_Menu):
    #opcion = variable que devuelve la función
    #opciones = lista de opciones
    #texto_Menu = texto del input
    opcion = 0
    i = 0
    for x in opciones:
        i = i + 1
        print(i,".",x)
    while opcion<1 or opcion>len(opciones):
        try: 
            opcion = int(input(texto_Menu))
            if opcion<1 or opcion>len(opciones):
                print("Por favor ingrese una opción válida")
        except:
            print("Por favor ingrese un número entero")
    return opcion

def menuString(opciones, texto_Menu):
    #opcion = variable que devuelve la función
    #opciones = lista de opciones
    #texto_Menu = texto del input
    opcion = 0
    i = 0
    for x in opciones:
        i = i + 1
        print(i,".",x)
    while opcion<1 or opcion>len(opciones):
        try: 
            opcion = int(input(texto_Menu))
            if opcion<1 or opcion>len(opciones):
                print("Por favor ingrese una opción válida")
        except:
            print("Por favor ingrese un número entero")
    return opciones[opcion-1]

def datoEstequiometria(reaccion, compuesto):
    print("¿Qué tipo de dato es? ")
    tipoDato = menu(["Masa","Volumen Líquido","Volumen Solución","Volumen de Gas"],"Ingrese el número de la opción correspondiente: ")
    if reaccion != None:
        print("¿De qué compuesto?")
        compuesto = menuString(reaccion.ObtenerReactivosString()+reaccion.ObtenerProductosString(),"Ingrese el número del compuesto: ")
    C = Conversiones()
    if tipoDato==1:
        dimensionales = menuString(C.listaDimensionales("Masa"),"Ingrese  el número de la unidad correspondiente: ")
        magnitud = input("Ingrese la magnitud de la masa: ")
        dato = Masa(compuesto=compuesto,dimensional=dimensionales,magnitud=magnitud)
    if tipoDato==2:
        print("Dimensionales del volumen:")
        dimensionales = menuString(C.listaDimensionales("Volumen"),"Ingrese  el número de la unidad correspondiente: ")
        magnitud = input("Ingrese la magnitud del volumen del líquido: ")
        print("Dimensionales de la densidad:")
        dimDensidad = menuString(C.listaDimensionales("Densidad"),"Ingrese  el número de la unidad correspondiente: ")
        densidad = input("Ingresela densidad del compuesto (si no la conoce,  ingrese 0): ")
        if densidad==0: densidad = None
        dato = Liquido(compuesto=compuesto,dimMagnitud=dimensionales,dimDensidad=dimDensidad,magnitud=magnitud,densidad=densidad)
    if tipoDato==3:
        print("Dimensionales del volumen:")
        dimensionales = menuString(C.listaDimensionales("Volumen"),"Ingrese  el número de la unidad correspondiente: ")
        magnitud = input("Ingrese la magnitud del volumen de la solución: ")
        molaridad = input("Ingrese la molaridad de la solución (M): ")
        dato = Solucion(compuesto=compuesto,dimensional=dimensionales,magnitud=magnitud,molaridad=molaridad)
    if tipoDato==4:
        print("Dimensionales del volumen:")
        dimensionales = menuString(C.listaDimensionales("Volumen"),"Ingrese  el número de la unidad correspondiente: ")
        magnitud = input("Ingrese la magnitud del volumen del gas: ")
        print("Dimensionales de la presión:")
        dimPresion =  menuString(C.listaDimensionales("Presion"),"Ingrese  el número de la unidad correspondiente: ")
        presion = input("Ingrese la magnitud de la presión: ")
        print("Dimensionales de la temperatura")
        dimTemp =  menuString(C.listaDimensionales("Temperatura"),"Ingrese  el número de la unidad correspondiente: ")
        temp = input("Ingrese la magnitud de la temperatura: ")
        dato = Gas(compuesto=compuesto,magnitud=magnitud,dimVolumen=dimensionales,dimPresion=dimPresion,
                   presion=presion,dimTemperatura=dimTemp,temperatura=temp)
    return dato


def incognitaEstequiometria(reaccion, compuesto, tipos):
    #Encontrar el tipo de incognita
    tipoDato = menu(tipos, "¿Qué desea encontrar? Ingrese el número correspondiente: ")
    if reaccion != None and tipoDato!="Porcentaje de rendimiento": #Si hay una reacción, pedir el compuesto
        print("¿De qué compuesto?") 
        compuesto = menuString(reaccion.ObtenerReactivosString()+reaccion.ObtenerProductosString(),"Ingrese el número del compuesto: ")
    C = Conversiones()
    if tipoDato==1: #Masa
        dimensionales = menuString(C.listaDimensionales("Masa"),"Ingrese el número de la unidad correspondiente: ")
        incognita = Masa(dimensional=dimensionales, compuesto=compuesto)
    if tipoDato==3:  #Volumen de líquido 
        magnitud, densidad = None, None
        print("Dimensionales del volumen:")
        dimensionales = menuString(C.listaDimensionales("Volumen"),"Ingrese  el número de la unidad correspondiente: ")
        print("Dimensionales de la densidad (si no conoce la densidad, elija g/L):")
        dimDensidad = menuString(C.listaDimensionales("Densidad"),"Ingrese  el número de la unidad correspondiente: ")
        densidad = input("Ingresela densidad del compuesto (si no la conoce,  ingrese 0): ")
        if densidad==0: densidad = None
        incognita = Liquido(compuesto=compuesto,dimMagnitud=dimensionales,dimDensidad=dimDensidad,magnitud=magnitud,densidad=densidad)
    if tipoDato==2 or tipoDato==5: #Volumen de solución o molaridad
        magnitud, molaridad = None, None
        print("Dimensionales del volumen:")
        dimensionales = menuString(C.listaDimensionales("Volumen"),"Ingrese  el número de la unidad correspondiente: ")
        if tipoDato!=2:
            magnitud = input("Ingrese la magnitud del volumen de la solución: ")
        if tipoDato != 5:
            molaridad = input("Ingrese la molaridad de la solución (M): ")
        incognita = Solucion(compuesto=compuesto,dimensional=dimensionales,magnitud=magnitud,molaridad=molaridad)
    if tipoDato==4 or tipoDato==6 or tipoDato==7: #Volumen, presion o temperatura de un gas)
        magnitud, presion, temp = None, None, None
        print("Dimensionales del volumen:")
        dimensionales = menuString(C.listaDimensionales("Volumen"),"Ingrese  el número de la unidad correspondiente: ")
        if  tipoDato!=4:
            magnitud = input("Ingrese la magnitud del volumen del gas: ")
        print("Dimensionales de la presión:")
        dimPresion =  menuString(C.listaDimensionales("Presion"),"Ingrese  el número de la unidad correspondiente: ")
        if tipoDato != 6:
            presion = input("Ingrese la magnitud de la presión: ")
        print("Dimensionales de la temperatura")
        dimTemp =  menuString(C.listaDimensionales("Temperatura"),"Ingrese  el número de la unidad correspondiente: ")
        if  tipoDato != 7:
            temp = input("Ingrese la magnitud de la temperatura: ")
        incognita = Gas(compuesto=compuesto,magnitud=magnitud,dimVolumen=dimensionales,dimPresion=dimPresion,
                   presion=presion,dimTemperatura=dimTemp,temperatura=temp)
    if tipoDato==8: #Cantidad de materia
        dimensionales = menuString(C.listaDimensionales("Cantidad Materia"),"Ingrese el número de la unidad correspondiente: ")
        incognita = Moles(dimensional=dimensionales, compuesto=compuesto)
    return incognita, tipos[tipoDato]

print("Gracias por ser un beta tester de nuestra app de química! Probaremos la función de estequiometría")
continuar = True
while(continuar):
    print("""Escribe la reacción química sin balancear separada por un signo =.  
          Por ejemplo: H2 + O2 = H2O
          Si no te dan una reacción (si no solo un compuesto) escribe su fórmula.
          Por ejemplo: H2O
          """)
    strReaccion = input()  #input de la fórmula
    reaccion  = Reaccion(strReaccion) if "=" in strReaccion else None #Creación del objeto reacción
    compuesto = compuesto(strReaccion) if "=" not in  strReaccion else None #Creación del objeto compuesto
    tipos = ["Masa", "Volumen de solución", "Volumen de líquido","Volumen de gas","Molaridad","Presión de un gas",
             "Temperatura de un gas", "Cantidad de materia"]
    if  reaccion != None: tipos.append("Porcentaje de rendimiento")
    incognita, tipoIncognita = incognitaEstequiometria(reaccion,compuesto,tipos) #Pedir la incógnita
    if tipoIncognita == 8: #Si la incognita es el porcentaje de rendimiento, pedir el dato real
        print("Ingrese el dato \"real\" del problema:")
        real = datoEstequiometria(reaccion, compuesto)
        real.setTeorico(False) 
    else: #Pedir el porcentaje de rendimiento
        print("Si en el problema te dan un porcentaje de rendimiento, ingrésalo. Si no te lo dan, presiona enter")
        porcentaje = input()
        porcentaje = float(porcentaje) if porcentaje != ""  else 100
    #Pedir los datos
    print("¿Cuántos datos más te dan en el problema? Escribe el número a continuación:")
    numDatos = int(input())
    datos = []
    for i in range(numDatos):
        print(f"Dato {i+1}:")
        datos.append(datoEstequiometria(reaccion, compuesto))
    problema = problemaEsteq(Datos=datos,Incognita=incognita,reaccion=reaccion, compuesto=compuesto, tipo = tipoIncognita, Rendimiento=porcentaje)
    print("\n Respuesta: \n" + problema.Respuesta())
    
    print("\n¿Desea resolver otro problema?")
    respuesta = menu(["Sí", "No"],"Escriba el número de la opción correspondiente: ")
    continuar =  respuesta == 1
print("Gracias por tu ayuda!")