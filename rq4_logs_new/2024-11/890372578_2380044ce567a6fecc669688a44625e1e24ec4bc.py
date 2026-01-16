# Variables para guardar los datos
temp = 0
luz = 0

# Función para mostrar un saludo con un corazón latiendo
def show_heart_greeting():
    for _ in range(3):  # Hacer que el corazón lata 3 veces
        basic.show_icon(IconNames.HEART)
        basic.pause(500)
        basic.clear_screen()
        basic.pause(500)
    basic.show_string("Hola!")

# Mostrar el saludo con el corazón latiendo
show_heart_greeting()

# Medir y mostrar la temperatura al encender
temp = input.temperature()
basic.show_string(f"Temp: {temp}C")

# Función principal que se ejecuta continuamente
def on_forever():
    global temp, luz

    # --- Mostrar nivel de luz ---
    luz = input.light_level()
    led.plot_bar_graph(luz, 255)
    basic.pause(1000)

    # --- Reproducir música y encender luces ---
    music.play(music.string_playable("G B A G C5 B A B ", 120), music.PlaybackMode.UNTIL_DONE)
    maqueen.write_led(maqueen.LED.LED_LEFT, maqueen.LEDswitch.TURN_ON)
    maqueen.write_led(maqueen.LED.LED_RIGHT, maqueen.LEDswitch.TURN_ON)
    basic.pause(2000)
    maqueen.write_led(maqueen.LED.LED_LEFT, maqueen.LEDswitch.TURN_OFF)
    maqueen.write_led(maqueen.LED.LED_RIGHT, maqueen.LEDswitch.TURN_OFF)

    # --- Ir hacia el objeto ---
    maqueen.motor_run(maqueen.Motors.ALL, maqueen.Dir.CW, 50)
    basic.pause(2000)
    maqueen.motor_stop(maqueen.Motors.ALL)
    basic.pause(500)

    # --- Bajar la pala para recoger el objeto ---
    maqueen.servo_run(maqueen.Servos.S1, 120)
    basic.pause(1000)

    # --- Avanzar un poco más para cargar el objeto ---
    maqueen.motor_run(maqueen.Motors.ALL, maqueen.Dir.CW, 50)
    basic.pause(1000)
    maqueen.motor_stop(maqueen.Motors.ALL)
    basic.pause(500)

    # --- Subir la pala con el objeto ---
    maqueen.servo_run(maqueen.Servos.S1, 60)
    basic.pause(1000)

    # --- Retroceder hacia el punto de partida ---
    maqueen.motor_run(maqueen.Motors.ALL, maqueen.Dir.CCW, 50)
    basic.pause(1000)

    # --- Dar media vuelta (girar 180 grados) ---
    maqueen.motor_run(maqueen.Motors.M1, maqueen.Dir.CW, 50)
    maqueen.motor_run(maqueen.Motors.M2, maqueen.Dir.CCW, 50)
    basic.pause(1500)
    maqueen.motor_stop(maqueen.Motors.ALL)
    basic.pause(500)

    # --- Volver al punto de partida ---
    maqueen.motor_run(maqueen.Motors.ALL, maqueen.Dir.CW, 50)
    basic.pause(2000)
    maqueen.motor_stop(maqueen.Motors.ALL)

# Ejecutar la función continuamente
basic.forever(on_forever)