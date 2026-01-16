import random

opciones = ["piedra", "papel", "tijera"]
jugando = True

def determinar_ganador(opcion_jugador, opcion_computadora):
    if opcion_jugador == opcion_computadora:
        return "Es un empate."
    elif (opcion_jugador == "piedra" and opcion_computadora == "tijera") or \
         (opcion_jugador == "papel" and opcion_computadora == "piedra") or \
         (opcion_jugador == "tijera" and opcion_computadora == "papel"):
        return "¡Ganaste!"
    else:
        return "Perdiste."

while jugando:
    opcion_jugador = input("Elige piedra, papel o tijera: ").lower()
    
    if opcion_jugador not in opciones:
        print("Opción inválida, intenta nuevamente.")
        continue
    
    opcion_computadora = random.choice(opciones)
    
    resultado = determinar_ganador(opcion_jugador, opcion_computadora)
    
    print(f"Tu opción: {opcion_jugador}")
    print(f"Opción de la computadora: {opcion_computadora}")
    print(f"Resultado: {resultado}")
    
    for i in range(3):
        print(f"¿Quieres seguir jugando? ({3-i}) intentos restantes.")
    
    seguir_jugando = input("¿Quieres jugar otra vez? (s/n): ").lower()
    if seguir_jugando != "s":
        jugando = False

print("Gracias por jugar. ¡Hasta la próxima!")