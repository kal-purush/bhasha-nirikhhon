# Aqui definimos las funciones que generan las piezas, cada pieza tiene su propio movimiento dentro de la funcion
def piezaBarra():
    global pulsar, x, y, linea
    pulsar = 2
    x = 1
    # Declaramos las posiciones iniciales
    y = 0
    finJuego()
    led.plot(x, y)
    led.plot(x + 1, y)
    basic.pause(500)
    for i in range(4):
        basic.pause(500)
        led.unplot(x, y)
        led.unplot(x + 1, y)
        y = y + 1
        if posicion[y][x] == 6 or posicion[y][x + 1] == 6:
            # Verificamos si hay colision en la proxima posicion
            y = y - 1
            led.plot(x, y)
            led.plot(x + 1, y)
            basic.pause(500)
            break
        else:
            # indica que creemos una pieza nueva pues ya ha parado de moverse esta
            led.plot(x, y)
            led.plot(x + 1, y)
            basic.pause(500)
    posicion[y][x] = 6
    posicion[y][x + 1] = 6
    linea[y] = linea[y] + 12
    verificarLinea()
    basic.pause(400)
    jugar()


def piezaPunto():
    global pulsar, x, y, linea
    pulsar = 1
    x = 1
    # Declaramos las posiciones iniciales
    y = 0
    finJuego()
    led.plot(x, y)
    basic.pause(500)
    for j in range(4):
        basic.pause(500)
        led.unplot(x, y)
        y = y + 1
        if posicion[y][x] == 6:
            # Verificamos si hay colision en la proxima posicion
            y = y - 1
            led.plot(x, y)
            basic.pause(500)
            break
        else:
            # indica que creemos una pieza nueva pues ya ha parado de moverse esta
            led.plot(x, y)
            basic.pause(500)
    posicion[y][x] = 6
    linea[y] = linea[y] + 6
    verificarLinea()
    basic.pause(400)
    jugar()


def piezaAnguloI():
    global pulsar, x, y, linea
    pulsar = 3
    x = 1
    # Declaramos las posiciones iniciales
    y = 0
    finJuego()
    led.plot(x, y)
    led.plot(x + 1, y)
    led.plot(x + 1, y + 1)
    basic.pause(500)
    for k in range(3):
        basic.pause(500)
        led.unplot(x, y)
        led.unplot(x + 1, y)
        led.unplot(x + 1, y + 1)
        y = y + 1
        if (
            posicion[y][x] == 6
            or posicion[y][x + 1] == 6
            or posicion[y + 1][x + 1] == 6
        ):
            # Verificamos si hay colision en la proxima posicion
            y = y - 1
            led.plot(x, y)
            led.plot(x + 1, y)
            led.plot(x + 1, y + 1)
            basic.pause(500)
            break
        else:
            # indica que creemos una pieza nueva pues ya ha parado de moverse esta
            led.plot(x, y)
            led.plot(x + 1, y)
            led.plot(x + 1, y + 1)
            basic.pause(500)
    posicion[y][x] = 6
    posicion[y][x + 1] = 6
    posicion[y + 1][x + 1] = 6
    linea[y] = linea[y] + 12
    linea[y + 1] = linea[y + 1] + 6
    verificarLinea()
    basic.pause(400)
    jugar()


def piezaAnguloE():
    global pulsar, x, y, linea
    pulsar = 4
    x = 1
    # Declaramos las posiciones iniciales
    y = 0
    # Verificamos si al crear la pieza esta te hace perder
    finJuego()
    led.plot(x, y)
    led.plot(x, y + 1)
    led.plot(x + 1, y)
    basic.pause(500)
    # Bucle que mueve la pieza
    for k in range(3):
        basic.pause(500)
        led.unplot(x, y)
        led.unplot(x, y + 1)
        led.unplot(x + 1, y)
        y = y + 1
        # Verificacion de colisiones
        if posicion[y][x] == 6 or posicion[y][x + 1] == 6 or posicion[y + 1][x] == 6:
            # Verificamos si hay colision en la proxima posicion
            y = y - 1
            led.plot(x, y)
            led.plot(x, y + 1)
            led.plot(x + 1, y)
            basic.pause(500)
            break

        else:
            # indica que creemos una pieza nueva pues ya ha parado de moverse esta
            led.plot(x, y)
            led.plot(x, y + 1)
            led.plot(x + 1, y)
            basic.pause(500)
    # Se anota la posicion en la variable posicion
    posicion[y][x] = 6
    posicion[y][x + 1] = 6
    posicion[y + 1][x] = 6
    # Se anota el valor de los pixeles encendidos en la variable linea
    linea[y] = linea[y] + 12
    linea[y + 1] = linea[y + 1] + 6
    # Verificamos si al depositar la pieza se debe borrar una linea
    verificarLinea()
    basic.pause(400)
    jugar()


def finJuego():
    global x, y, posicion, pulsar
    if pulsar == 1 and (posicion[y][x] == 6 or y == 4):
        basic.show_string("Game Over")
        basic.pause(2000)
        iniciarPartida()
    elif pulsar == 2 and (posicion[y][x] == 6 or posicion[y][x + 1] == 6):
        basic.show_string("Game Over")
        basic.pause(2000)
        iniciarPartida()
    elif pulsar == 3 and (
        posicion[y][x] == 6 or posicion[y][x + 1] == 6 or posicion[y + 1][x + 1] == 6
    ):
        basic.show_string("Game Over")
        basic.pause(2000)
        iniciarPartida()
    elif pulsar == 4 and (
        posicion[y][x] == 6 or posicion[y][x + 1] == 6 or posicion[y + 1][x] == 6
    ):
        basic.show_string("Game Over")
        basic.pause(2000)
        iniciarPartida()


def iniciarPartida():
    # Reinicia todas las variables y comienza una nueva partida
    global y, x, pulsar, numeroAnterior, lineaTemporal, posicion, linea
    y = 0
    x = 0
    pulsar = 0
    numeroAnterior = 0
    posicion = [
        [0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0],
        [0, 0, 0, 0, 0],
    ]
    linea = [0, 0, 0, 0, 0]
    jugar()


# fucion que verifica si la linea esta llena y si lo esta la borra
def verificarLinea():
    global x, y, posicion, linea, lineaTemporal

    # Lista para almacenar las filas completas que se eliminarán
    filas_completas = []

    # Verificar filas completas
    for h in range(5):
        if linea[h] == 30:
            filas_completas.append(h)
            music.play_tone(Note.C, music.beat(BeatFraction.HALF))

    # Eliminar las filas completas
    for fila in filas_completas:
        for j in range(5):
            led.unplot(j, fila)
            # Restablecer la línea a cero
            linea[fila] = 0
        # Mover las filas superiores hacia abajo
        for i in range(fila, 0, -1):
            for j in range(5):
                # Mover los LED hacia abajo en la matriz y en la pantalla
                posicion[i][j] = posicion[i - 1][j]
                if posicion[i][j] == 6:
                    led.plot(j, i)
                else:
                    led.unplot(j, i)
            # Actualizar la línea
            linea[i] = linea[i - 1]

    # Borrar la fila superior si es necesario
    if 30 in linea:
        for j in range(5):
            led.unplot(j, 0)
            posicion[0][j] = 0
        linea[0] = 0


def reproducirMusicaTetris():
    music.play_melody("E D E C D C B C5 ", 120)
    music.play_melody("B A B G A G F G ", 120)
    music.play_melody("F E F D E D C D ", 120)
    music.play_melody("C B C A B A G B ", 120)
    music.play_melody("E D E C D C B C5 ", 120)
    music.play_melody("B A B G A G F G ", 120)
    music.play_melody("F E F D E D C D ", 120)
    music.play_melody("C B C A B A G B ", 120)


# Funcion principal de juego
def jugar():
    global pulsar, numeroAnterior
    # generamos una pieza aleatoria
    pulsar = randint(1, 4)
    if pulsar == 3:
        piezaAnguloI()
    elif pulsar == 4:
        piezaAnguloE()
    elif pulsar == 2:
        piezaBarra()
    elif pulsar == 1:
        piezaPunto()
    # Repetir la música de Tetris
    reproducirMusicaTetris()


# Definimos el movimiento lateral de cada pieza


def pulsarA():
    global x
    # ponemos un codigo diferente segun la cantidad de pixeles que tenga la pieza
    # usamos la variable pulsar para verificar que pieza movemos
    if pulsar == 2 and x > 0 and posicion[y][x - 1] == 0 and posicion[y][x] == 0:
        led.unplot(x, y)
        led.unplot(x + 1, y)
        x = x - 1
        led.plot(x, y)
        led.plot(x + 1, y)
    elif (
        pulsar == 3
        and x > 0
        and posicion[y][x - 1] == 0
        and posicion[y][x] == 0
        and posicion[y + 1][x] == 0
    ):
        led.unplot(x, y)
        led.unplot(x + 1, y)
        led.unplot(x + 1, y + 1)
        x = x - 1
        led.plot(x, y)
        led.plot(x + 1, y)
        led.plot(x + 1, y + 1)
    elif pulsar == 1 and x > 0 and posicion[y][x - 1] == 0:
        led.unplot(x, y)
        x = x - 1
        led.plot(x, y)
    elif (
        pulsar == 4
        and x > 0
        and posicion[y][x - 1] == 0
        and posicion[y][x] == 0
        and posicion[y + 1][x - 1] == 0
    ):
        led.unplot(x, y)
        led.unplot(x + 1, y)
        led.unplot(x, y + 1)
        x = x - 1
        led.plot(x, y)
        led.plot(x + 1, y)
        led.plot(x, y + 1)


input.on_button_pressed(Button.A, pulsarA)


def pulsarB():
    global x
    # ponemos un codigo diferente segun la cantidad de pixeles que tenga la pieza
    # usamos la variable pulsar para verificar que pieza movemos
    if pulsar == 2 and x < 4 and posicion[y][x + 1] == 0 and posicion[y][x + 2] == 0:
        led.unplot(x, y)
        led.unplot(x + 1, y)
        x = x + 1
        led.plot(x, y)
        led.plot(x + 1, y)
    elif (
        pulsar == 3
        and x < 4
        and posicion[y][x + 1] == 0
        and posicion[y][x + 2] == 0
        and posicion[y + 1][x + 2] == 0
    ):
        led.unplot(x, y)
        led.unplot(x + 1, y)
        led.unplot(x + 1, y + 1)
        x = x + 1
        led.plot(x, y)
        led.plot(x + 1, y)
        led.plot(x + 1, y + 1)
    elif pulsar == 1 and x < 4 and posicion[y][x + 1] == 0:
        led.unplot(x, y)
        x = x + 1
        led.plot(x, y)
    elif (
        pulsar == 4
        and x < 4
        and posicion[y][x + 1] == 0
        and posicion[y][x + 2] == 0
        and posicion[y + 1][x + 1] == 0
    ):
        led.unplot(x, y)
        led.unplot(x + 1, y)
        led.unplot(x, y + 1)
        x = x + 1
        led.plot(x, y)
        led.plot(x + 1, y)
        led.plot(x, y + 1)


input.on_button_pressed(Button.B, pulsarB)

# definicion de las variables

y = 0
x = 0

pulsar = 0
numeroAnterior = 0
# En esta matriz almacenamos los leds encendidos de forma que podamos tener coliciones
posicion = [
    [0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0],
]

linea = [0, 0, 0, 0, 0]


jugar()