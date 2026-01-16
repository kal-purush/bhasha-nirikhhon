#Librerías
import pprint as pp
import numpy as np
from cargadorSoluciones import *
from cargadorInstancias import *
       
    
#Sección Principal
#-----------------

#Entrada por lotes (pendiente)
#Entrada por lotes (pendiente)
#Entrada por lotes (pendiente)

#Entradas para una sola instancia
instancia = beasley('./instances/csp50.txt')	
solucion = solucionVRP_Solver('./solutions/vrpSolver_csp50.txt')#Solución sospecha infactible, 24 itinerarios
#solucion = solucionVRP_Solver('./solutions/sol_csp50.txt')#Solución sospecha infactible, 24 itinerarios
#solucion = solucionVRP_Solver('./solutions/sol_csp50_b.txt')#Solución VRPSolver tiempos de servicio corregidos, 27 itinerarios
#solucion = solucionVRP_Solver('./solutions/sol_csp50_modeloPropuesto.txt')#Solución mesquita92+borcinova17, 27 itinerarios

#Recorrer los itinerarios y revisar:
#-Traslapes de ventanas de tiempo
#-Duración total del turno
#-Número de conductores (opcional)

#Contenedor del reporte
reporte = {}
reporte['traslapes'] = { 'numero':0, 'tuplas':[] }
reporte['itinerariosExtensos'] = { 'numero':0, 'itinerarios':[], 'excesos':[] }
reporte['costoInterno'] = 0
reporte['costoCompleto'] = 0

#Contenedor de control de los itinerarios
listadoBloques = {}
for i in range(1,instancia['numBloques']+1):
    listadoBloques[i] = 0

#Por cada uno de los itinerarios
indiceItinerario = 1
for itinerario in solucion['itinerarios']:
    
    #Acumulador de la duración del itinerario
    duracionItinerario = 0  
    
    #Acumulador de costo de cada itinerario (diagnóstico)
    costoItinerario = 0
    
    #Validar si se trata de un itinerario con bloque único
    if len(itinerario) == 1:
        
        #Incorporar el tiempo de servicio
        duracionItinerario += instancia['bloques'][ itinerario[0] - 1 ]['duracion']
        
        #Checkear el bloque en el listado de control
        listadoBloques[ itinerario[0] ] += 1       
    
    else:#Itinerarios con más de un bloque de trabajo o servicio
    
        #Por cada uno de los bloques del itinerario, revisar la transcición
        for i in range(len(itinerario)-1):
            
            #Tiempo de servicio del itinerario actual
            duracionItinerario += instancia['bloques'][ itinerario[i] - 1 ]['duracion']
            
            #Checkear los bloques en el listado de control
            listadoBloques[ itinerario[i] ] += 1            
            
            #Incorporar el costo de cada transición
            reporte['costoInterno'] += instancia['matrizCostos'][ itinerario[i] , itinerario[i+1] ]
            
            #Diagnóstico de costos parciales
            costoItinerario += instancia['matrizCostos'][ itinerario[i] , itinerario[i+1] ]
            
            #Revisar tiempo de transición (factibilidad)
            if not(instancia['matrizTiemposTransicion'][ itinerario[i] , itinerario[i+1] ] == instancia['M']):
                duracionItinerario += instancia['matrizTiemposTransicion'][ itinerario[i] , itinerario[i+1] ]
            else:#Si es infactible, añadirlo al reporte              
                
                #Incrementar el contador de traslapes
                reporte['traslapes']['numero'] += 1
                
                #Guardar la tupla con el id del itinerario y los bloques traslapados
                reporte['traslapes']['tuplas'].append( (indiceItinerario, itinerario[i] , itinerario[i+1] ) )
                
                #Reflejar infactibilidad en la duración del itinerario
                duracionItinerario = instancia['M']              
        
        
        #Agregar tiempo del último bloque del itinerario        
        duracionItinerario += instancia['bloques'][ itinerario[-1] - 1 ]['duracion']
        
        #Checkear el último del itinerario en la lista de control
        listadoBloques[ itinerario[-1] ] += 1
        
        #Revisar duración del itinerario, incorporar el detalle de la infactibilidad si se presenta
        if duracionItinerario > instancia['minutosJornada']:           
            reporte['itinerariosExtensos']['numero'] += 1           
            reporte['itinerariosExtensos']['itinerarios'].append(itinerario)
            reporte['itinerariosExtensos']['excesos'].append(duracionItinerario - instancia['minutosJornada'])
          
    #Acumular costo de salida para cada itinerario
    reporte['costoCompleto'] += instancia['matrizCostos'][ 0 , itinerario[0] ]
    #reporte['costoCompleto'] += 750 #Validación de la solución generada entre julio y septiembre 2020
    
    #Acumular el costo de cada itinerario para el costo completo
    reporte['costoCompleto'] += costoItinerario
    
    #Diagnóstico costo de cada itinerario    
    print(indiceItinerario,' - ',itinerario, ': ', costoItinerario)    
    
    #Incrementar el índice de itinerarios para el reporte de errores
    indiceItinerario += 1
    
#Incorporar al reporte los bloques ausentes y los repetidos
reporte['repetidos'] = []
reporte['ausentes'] = []
for bloque,apariciones in listadoBloques.items():
    if apariciones > 1:
        reporte['repetidos'] = bloque
    elif apariciones == 0:
        reporte['ausentes'] = bloque
    
#Mostrar reporte detallado
pp.pprint(reporte)