palabra = "radio"
respuesta = 's'
puntajes = {
    'partidas': 0, 'racha': 0, 'mejorRacha': 0,
    'aciertos': 0,
    '1': 0, '2': 0, '3': 0, '4': 0, '5': 0, '6': 0,
    'fallos': 0
}

def mostrarResultados():
    porcentajeVictorias = (puntajes['aciertos']/(puntajes['partidas']))*100
    print("\
        \nEstadísticas:\
            \nJugadas: %i | Victorias: %d (Porcentaje) | Racha Actual: %i | Mejor Racha: %i\
        \nDistribución:\
            \nIntento 1:\t%i\nIntento 2:\t%i\nIntento 3:\t%i\nIntento 4:\t%i\nIntento 5:\t%i\nIntento 6:\t%i\nFallos:\t\t%i"\
        % (puntajes['partidas'], porcentajeVictorias, puntajes['racha'], puntajes['mejorRacha'],\
        puntajes['1'], puntajes['2'], puntajes['3'], puntajes['4'], puntajes['5'], puntajes['6'], puntajes['fallos']))

def compararPalabras(ingreso):
    i = 0
    coincidencias = ''
    for x in ingreso:
        if x == palabra[i]:
            coincidencias += '='
        elif x in palabra:
            coincidencias += '-'
        else:
            coincidencias += ' '
        i += 1
    return coincidencias

def mostrarIntento(ingreso, coincidencias):
    for x in ingreso:
        print(x, end=' ')
    print()
    for x in coincidencias:
        print(x, end=' ')

def acertar():
    print("\nACERTASTE!!!")
    puntajes['aciertos'] += 1
    puntajes[intentos.__str__()] += 1
    if puntajes['mejorRacha'] == puntajes['racha']:
        puntajes['mejorRacha'] += 1
    puntajes['racha'] += 1

def fallar():
    print("\nFALLASTE!!!")
    print("La palabra era %s" % (palabra))
    puntajes['fallos'] += 1
    puntajes['racha'] = 0

while respuesta == 's':
    puntajes['partidas'] += 1
    intentos = 1
    while intentos <= 6:
        ingreso = ''
        print()
        while len(ingreso) != 5:
            ingreso = input("Ingrese palabra de 5 letras: ")
        ingreso = ingreso.lower()

        mostrarIntento(ingreso, compararPalabras(ingreso))
        
        if ingreso == palabra:
            acertar()
            intentos = 999
        elif intentos == 6:
            fallar()
            intentos = 999
        intentos += 1
    
    mostrarResultados()
    respuesta = ''
    print()
    if(respuesta != 's' or respuesta != 'n'):
        respuesta = input("¿Seguir jugando? (s = sí/n = no): ")
    