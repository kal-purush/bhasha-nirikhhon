def choose_level(n_pregunta, p_level):
    '''
    n_pregunta --> turno pregunta, asignación de nro
    p_level --> nro preguntas por nivel

    p_level = 1
    1.- basica         p == n_pregunta
    2.- media          2 * P == n_pregunta
    3.- avanzada       3 * p == n_pregunta

    p_level = 2
    1 y 2 basicas     1 * p_level >= n_pregunta  
    3 y 4 intermedia  2 * p_level >= n_pregunta
    5 y 6 avanzadas   2 * P_level < n_pregunta

    p_level = 3
    1,2,3 basica         1 * p_level 
    4,5,6 intermedia     2 * p_level
    7,8,9 avanzad        3 * p_level
    '''
    # Construir lógica para escoger el nivel
    if n_pregunta <= p_level:
      level = 'basicas'
    elif n_pregunta <= 2*p_level:
      level = 'intermedias'
    else: level = 'avanzadas'
    return level

if __name__ == '__main__':
    # verificar resultados
    print(choose_level(2, 2)) # básicas
    print(choose_level(3, 2)) # intermedias
    print(choose_level(7, 2)) # avanzadas
    print(choose_level(4, 3)) # intermedias