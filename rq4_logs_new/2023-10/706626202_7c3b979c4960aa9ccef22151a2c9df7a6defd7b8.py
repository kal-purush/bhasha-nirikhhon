# Grado en Ingeniería Informática en UDIMA
# Asignatura Progamación avanzada
# AA1 - Algoritmos genéticos
# Autor: Juan del Río Huertas
# --------------------------------------------


import numpy as np
import random
import matplotlib.pyplot as plt



tot_asistentes = 50
tot_talleres = 15
tot_horas = 20 # dias x horas
tot_poblacion = 10  # total individuos (soluciones).
capacidad_max_sala = 20
num_max_salas = 3
prob_asistir = 0.8
num_generaciones =1000000
prob_mutacion = 0.05


# -------------------------------------------------------------------
# Datos entrada / restricciones: elección de los talleres que quiere cada asistente
# -------------------------------------------------------------------
asistentes_talleres = np.zeros((tot_asistentes,tot_talleres)) 

# elegir aleatoriamente los datos
for asistente in range(tot_asistentes):
    for taller in range(tot_talleres):
        if (random.random() < prob_asistir ):
            asistentes_talleres[asistente,taller]= 1
        else:
            asistentes_talleres[asistente,taller]= 0
            
# Posibilidad de guardar e importar datos de entrada
# np.save('asistentes_talleres' , asistentes_talleres)  
# entrada = np.load('asistentes_talleres.npy')  

# -------------------------------------------------------------------
# Iniciar aleatoriamente la primera población
# -------------------------------------------------------------------
poblacion = np.zeros((tot_poblacion,tot_asistentes, tot_talleres))


print("Población= nº indiv,nº asistentes,nºsalas =")
print(poblacion.shape)

# Obtener población inicial aleatoria
for individuo in range(tot_poblacion):
    for asistente in range(tot_asistentes):
        for taller in range(tot_talleres):
            if (random.random()<prob_asistir):
                poblacion[individuo,asistente,taller]=random.randrange(tot_horas)    
        

# Funcion Evaluación
def evaluar_individuo(individuo):
    
    defectos = 0
    
    # crear tabla asistentes_talleres_ocupados
    asistentes_talleres_ocupados = np.zeros((tot_asistentes, tot_talleres))  
    
    for asistente in range(tot_asistentes):        
        for taller in range(tot_talleres):
            if (individuo [asistente,taller] > 0):
                asistentes_talleres_ocupados [asistente,taller] = asistentes_talleres_ocupados [asistente,taller] + 1
               

    # evaluar según tabla asistentes_talleres_ocupados: diferencia de valores 
    for asistente in range(tot_asistentes):        
        for taller in range(tot_talleres):            
            defectos = defectos + abs(asistentes_talleres[asistente,taller]-asistentes_talleres_ocupados [asistente,taller])
             
    
    horas_talleres_ocupados = np.zeros((tot_horas,tot_talleres))      
    for asistente in range(tot_asistentes):         
        for taller in range(tot_talleres):
            if (individuo [asistente,taller] > 0):                
                horas_talleres_ocupados [int(individuo [asistente,taller])-1,taller] = horas_talleres_ocupados [int(individuo [asistente,taller])-1,taller] + 1      
                
    # evaluar según tabla horas_talleres_ocupados: diferencia de valores respecto capacidad_max_sala            
    for hora in range(tot_horas):        
        for taller in range(tot_talleres):
            if (horas_talleres_ocupados [hora,taller] > capacidad_max_sala ):
                defectos = defectos + (horas_talleres_ocupados [hora,taller]-capacidad_max_sala)             
    
    # evaluar según tabla horas_talleres_ocupados: diferencia de valores respecto num_max_salas  
    for hora in range(tot_horas):     
        tot_salas_por_hora = 0
        for taller in range(tot_talleres):
            if (horas_talleres_ocupados [hora,taller] > 0):
                tot_salas_por_hora = tot_salas_por_hora + 1
        if ( tot_salas_por_hora > num_max_salas ):
            defectos = defectos + tot_salas_por_hora - num_max_salas
            
    return defectos 

# Ranking de individuos según evaluación
def obtener_ranking():        
    
    # Evaluar población
    ranking_poblacion= np.zeros((tot_poblacion,3))
    for individuo in range(tot_poblacion):
        ranking_poblacion[individuo,0] = int(individuo)
        ranking_poblacion[individuo,1] = int(evaluar_individuo(poblacion[individuo,:,:]))  
        ranking_poblacion[individuo,2] = int(evaluar_individuo(poblacion[individuo,:,:]))  
    
    # Ordenar según evaluación
    
    valor_maximo= int( tot_asistentes * tot_talleres *0.8)  # np.max(ranking_poblacion, axis=0)[1]
    
    # Invertir valores
    for individuo in range(tot_poblacion):    
        ranking_poblacion[individuo,2] = int(valor_maximo-ranking_poblacion[individuo,2]) 
    # Ordenar según el valor invertido
    ranking_poblacion_ord = ranking_poblacion[ranking_poblacion[:, 2].argsort()]
    # Calcular valores acumulados
    for individuo in range(tot_poblacion):
        if (individuo>0):
            ranking_poblacion_ord[individuo,2] = ranking_poblacion_ord[individuo,2]+ranking_poblacion_ord[individuo-1,2]       
    
    return ranking_poblacion_ord
        

def get_individuo(r,ranking):
    
    total_valoraciones = np.sum(ranking, axis=0)[1]
    for individuo in range(tot_poblacion):  
        if(individuo< tot_poblacion-1): 
            if (total_valoraciones!=0):
                if (ranking[individuo,1]/total_valoraciones>= r):                 
                    return poblacion[int(ranking[individuo,0]),:,:]   
            else:
                return poblacion[int(ranking[individuo,0]),:,:]
        else:
            return poblacion[int(ranking[individuo,0]),:,:]
    

# -------------------------------------------------------------------
# Bucle principal algoritmo genético
# -------------------------------------------------------------------
    
no_cumple_criterio_terminacion = True
generacion = 1
# Vectores para generar gráfico
x = []
y = []

while no_cumple_criterio_terminacion:
    
    #Crear población temporal
    poblacion_temp = np.zeros((tot_poblacion,tot_asistentes,tot_talleres))
    ranking = obtener_ranking()
    print("Gen: ",generacion,", mínimo: ",ranking[tot_poblacion-1,1])
   
    total_individos = 0
    while total_individos < tot_poblacion:
        
        # Seleccionar padres por ruleta 
        padre1 = get_individuo(random.random(),ranking)    
        padre2 = get_individuo(random.random(),ranking)  
        
        # cruce 
        hijo=  np.zeros((tot_asistentes, tot_talleres))
        for asistente in range(tot_asistentes):        
            for taller in range(tot_talleres):
                if (random.random() < 0.5):
                    hijo[asistente,taller]=padre1[asistente,taller]
                else:
                    hijo[asistente,taller]=padre2[asistente,taller]
                  
        # mutación
        if(random.random()<prob_mutacion):
            hijo[random.randrange(tot_asistentes),random.randrange(tot_talleres)]=random.randrange(tot_horas)
       
        # Añadir hijos a población                        
        for asistente in range(tot_asistentes):        
            for taller in range(tot_talleres):
                poblacion_temp[total_individos,asistente,taller]=hijo[asistente,taller]      
               
        total_individos = total_individos + 1
    
    poblacion = poblacion_temp
    generacion =  generacion + 1 
    ranking = obtener_ranking()
    
    x.append(generacion)
    y.append(ranking[tot_poblacion-1,1]) 
        
    if(generacion==num_generaciones or ranking[tot_poblacion-1,2]==0 ):        
        no_cumple_criterio_terminacion = False  
        
        
# Finalización: mostrar resultado obtenido y gráfico de las generaciones
print("Mínimo conseguido",ranking[tot_poblacion-1,1])    
plt.plot(x, y)
plt.show()        
        
    
    
    
    