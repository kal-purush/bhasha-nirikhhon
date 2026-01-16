import pygame
import random
import pyautogui
import pyttsx3
import sys
import os

ruta_base =         os.path.dirname(os.path.abspath(__file__))
# Inicializar pygame
pygame.init()
engine = pyttsx3.init()
pygame.mixer.set_num_channels(9)

# Configuración de la ventana y la grilla
# WIDTH, HEIGHT = 450, 450  # Tamaño de la ventana
# ROWS, COLS = 9, 9         # Tamaño de la grilla 9x9
# CELL_SIZE = WIDTH // COLS  # Tamaño de cada celda
ROWS, COLS = 9, 9
CELL_SIZE = 90
WIDTH, HEIGHT = COLS * CELL_SIZE, ROWS * CELL_SIZE


# Colores
WHITE = (255, 255, 255)
BLACK = (0, 0, 0)
GRAY = (192, 192, 192)
DARK_GRAY = (160, 160, 160)
RED = (255, 0, 0)

# Cargar sonidos
pygame.mixer.init()
sounds = [pygame.mixer.Sound(f"sounds/soundsB/sonido_{i+1}.wav") for i in range(9)]
channels = [pygame.mixer.Channel(i) for i in range(9)]  # Asignar un canal para cada sonido
explosion_sound = pygame.mixer.Sound('sounds/soundsB/explosion.wav')
channel1 = pygame.mixer.Channel(0)
# Configurar la pantalla
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Grilla de Sonidos Espaciales")

sound1 = pygame.mixer.Sound(ruta_base+'/sounds/sonido_1.wav')

# Estados iniciales
revealed = [[False for _ in range(COLS)] for _ in range(ROWS)]
sound_grid = [[None for _ in range(COLS)] for _ in range(ROWS)]  # Grilla que contiene los índices de sonido
first_click = False  # Para determinar si ya se hizo el primer clic
current_sound_playing = None  # Para rastrear el sonido actual
current_center = None  # Mantener la celda central actual
# Variable para guardar el sonido activo
current_sound = None

# Crear una matriz de minas y números
mines = [[0 for _ in range(COLS)] for _ in range(ROWS)]
revealed = [[False for _ in range(COLS)] for _ in range(ROWS)]

# Colocar minas aleatoriamente
NUM_MINES = 10
mines_pos = random.sample([(r, c) for r in range(ROWS) for c in range(COLS)], NUM_MINES)
for r, c in mines_pos:
    mines[r][c] = -1  # -1 representa una mina


# Calcular los números que indican cuántas minas hay alrededor de cada celda
def calculate_mines_around():
    directions = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]
    for r in range(ROWS):
        for c in range(COLS):
            if mines[r][c] == -1:
                continue
            count = 0
            for dr, dc in directions:
                nr, nc = r + dr, c + dc
                if 0 <= nr < ROWS and 0 <= nc < COLS and mines[nr][nc] == -1:
                    count += 1
            mines[r][c] = count

# Decir cuántas minas hay alrededor de la celda
def speak_mines_around(row, col):
    if mines[row][col] != -1:
        num_mines = mines[row][col]
        engine.say(f"Hay {num_mines} minas alrededor")
        engine.runAndWait()

   # Terminar el juego cuando explote una mina
def game_over():
    explosion_sound.play()
    engine.say("Has explotado una mina. Fin del juego.")
    engine.runAndWait()
    explosion_sound.stop()
    pygame.quit()
    exit()

def is_mouse_inside_window(mouse_x, mouse_y):
    if (1 <= mouse_x < WIDTH - 1 and 1 <= mouse_y < HEIGHT - 1):
        return True
    else:
        return False  
    


# Función para asignar sonidos a la grilla alrededor de la central
def assign_sounds_to_grid(center_row, center_col):
    global current_center
    # Lista de sonidos en el orden especificado
    sound_order = [2, 3, 4, 5, 1, 6, 7, 8, 9]

    # Direcciones relativas a la celda central
    directions = [(-1, -1), (-1, 0), (-1, 1),  # Filas superiores
                  (0, -1),  (0, 0),  (0, 1),  # Fila del medio
                  (1, -1),  (1, 0),  (1, 1)]  # Filas inferiores

    # Limpiar la grilla de sonidos
    for r in range(ROWS):
        for c in range(COLS):
            sound_grid[r][c] = None

    # Asignar sonidos a las celdas alrededor del centro
    for idx, (dr, dc) in enumerate(directions):
        new_row = center_row + dr
        new_col = center_col + dc
        if 0 <= new_row < ROWS and 0 <= new_col < COLS:
            sound_grid[new_row][new_col] = sound_order[idx]
    # Actualizar la celda central
    current_center = (center_row, center_col)


# Función para dibujar la grilla
def draw_grid():
    for r in range(ROWS):
        for c in range(COLS):
            rect = pygame.Rect(c * CELL_SIZE, r * CELL_SIZE, CELL_SIZE, CELL_SIZE)
            if revealed[r][c]:
                pygame.draw.rect(screen, GRAY, rect)
                if mines[r][c] == -1:
                    pygame.draw.circle(screen, RED, rect.center, CELL_SIZE // 4)
                elif mines[r][c] > 0:
                    font = pygame.font.SysFont(None, 36)
                    text = font.render(str(mines[r][c]), True, BLACK)
                    screen.blit(text, (c * CELL_SIZE + 20, r * CELL_SIZE + 15))
            else:
                pygame.draw.rect(screen, DARK_GRAY, rect)
            pygame.draw.rect(screen, BLACK, rect, 2)

# Función para manejar clics en la grilla
def handle_click(pos):
    global first_click

    x, y = pos
    row = y // CELL_SIZE
    col = x // CELL_SIZE

    # Si es el primer clic, configurar los sonidos en torno a la grilla seleccionada
    if not first_click:
        assign_sounds_to_grid(row, col)
        revealed[row][col] = True  # Revelar la celda central
        first_click = True
    else:
        # Si no es el primer clic, verificar si se hace clic en una celda circundante
        if sound_grid[row][col] is not None and (row, col) != current_center:
            # Establecer la nueva celda como central
            revealed[row][col] = True  # Revelar la nueva celda central
            assign_sounds_to_grid(row, col)  # Redistribuir los sonidos

# Función para manejar el sonido espacial según la posición del mouse
def play_sound_based_on_mouse(mouse_x, mouse_y, sound_index, channel):
    # Normalizar la posición x del mouse a un rango de [-1, 1]
    x = (mouse_x / WIDTH) * 2 - 1  # Izquierda = -1, Derecha = 1
    # Normalizar la posición y del mouse a un rango de [0, 1] (para simular distancia)
    y = mouse_y / HEIGHT  # Arriba = cercano, Abajo = lejano

    # Controlar el balance estéreo en función de la posición en el eje X
    left_volume = max(0, 1 - x)
    right_volume = max(0, 1 + x)

    # Reducir volumen en función de la distancia (simulación simple en Y)
    distance_factor = max(0.2, 1 - y)

    # Ajustar el volumen para el balance estéreo
    channel.set_volume(left_volume * distance_factor, right_volume * distance_factor)

    # Reproducir el sonido en el canal si no está ya ocupado
    if not channel.get_busy():
        channel.play(sounds[sound_index])

# Función para manejar el sonido según la posición del mouse
def handle_mouse_movement(pos):
    global current_sound_playing

    x, y = pos
    row = y // CELL_SIZE
    col = x // CELL_SIZE

    # Verificar si el mouse está dentro de una celda válida con sonido
    if 0 <= row < ROWS and 0 <= col < COLS and sound_grid[row][col] is not None:
        sound_index = sound_grid[row][col] - 1  # Obtener el índice del sonido correspondiente

        # Solo reproducir si no es el mismo sonido que ya está sonando
        if current_sound_playing != sound_index:
            # Detener el sonido anterior si es necesario
            if current_sound_playing is not None:
                channels[current_sound_playing].stop()

            # Reproducir el nuevo sonido
            play_sound_based_on_mouse(x, y, sound_index, channels[sound_index])
            current_sound_playing = sound_index

    else:
        # Detener el sonido si el mouse sale de las celdas con sonido
        if current_sound_playing is not None:
            channels[current_sound_playing].stop()
            current_sound_playing = None


def get_cell_under_mouse():
    x, y = pygame.mouse.get_pos()
    row = y // CELL_SIZE
    col = x // CELL_SIZE
    return row, col

def play_sound_based_on_mouse1(mouse_x, mouse_y, sound):
    # Normalizar la posición x del mouse a un rango de [-1, 1]
    x = (mouse_x / WIDTH) * 2 - 1  # Izquierda = -1, Derecha = 1
    # Normalizar la posición y del mouse a un rango de [0, 1] (para simular distancia)
    y = mouse_y / HEIGHT  # Arriba = cercano, Abajo = lejano

    # Controlar el balance estéreo en función de la posición en el eje X
    left_volume = max(0, 1 - x)
    right_volume = max(0, 1 + x)

    # Reducir volumen en función de la distancia (simulación simple en Y)
    distance_factor = max(0.1, 1 - y)

    # Ajustar el volumen para el balance estéreo
    channel1.set_volume(left_volume * distance_factor, right_volume * distance_factor)

    # Reproducir el sonido en el canal
    if not channel1.get_busy():
        channel1.play(sound)

calculate_mines_around()
# Bucle principal del juego
running = True
mouse_inside_window = True
while running:
    screen.fill(GRAY)
    draw_grid()
    

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.MOUSEBUTTONDOWN:
            row, col = get_cell_under_mouse()
            if not revealed[row][col]:
                revealed[row][col] = True
                if mines[row][col] == -1:
                    game_over()
                else:
                    speak_mines_around(row, col)
            handle_click(pygame.mouse.get_pos())
    # Verificar la posición del mouse constantemente
    handle_mouse_movement(pygame.mouse.get_pos())
    
    # Obtener la posición del mouse
    mouse_x, mouse_y = pygame.mouse.get_pos()

    # Verificar si el mouse está dentro de la ventana
    if is_mouse_inside_window(mouse_x, mouse_y) and first_click == False:
        if not mouse_inside_window:  # Si el mouse acaba de volver a la ventana
            mouse_inside_window = True  # Actualizamos el estado del mouse
        play_sound_based_on_mouse1(mouse_x, mouse_y, sound1)
    
    else:
        if mouse_inside_window:  # Si el mouse acaba de salir de la ventana
            channel1.stop()  # Detenemos el sonido
            mouse_inside_window = False


    pygame.display.flip()

pygame.quit()
sys.exit()