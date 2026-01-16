import sys
import json
import os

# Ruta del archivo JSON donde se guardan las tareas
ARCHIVO_TAREAS = "tareas.json"


def cargar_tareas():
    """Carga las tareas desde el archivo JSON."""
    if not os.path.exists(ARCHIVO_TAREAS):
        return []
    with open(ARCHIVO_TAREAS, "r") as archivo:
        return json.load(archivo)


def guardar_tareas(tareas):
    """Guarda las tareas en el archivo JSON."""
    with open(ARCHIVO_TAREAS, "w") as archivo:
        json.dump(tareas, archivo, indent=4)


def listar_tareas(estado_filtro=None):
    """Lista todas las tareas, opcionalmente filtrando por estado."""
    tareas = cargar_tareas()
    if estado_filtro:
        tareas = [tarea for tarea in tareas if tarea["estado"] == estado_filtro]
    for i, tarea in enumerate(tareas, start=1):
        print(f"{i}. {tarea['titulo']} - {tarea['estado']}")


def agregar_tarea(titulo):
    """Agrega una nueva tarea."""
    tareas = cargar_tareas()
    tareas.append({"titulo": titulo, "estado": "pendiente"})
    guardar_tareas(tareas)
    print(f"Tarea '{titulo}' agregada.")


def actualizar_tarea(indice, nuevo_titulo):
    """Actualiza el título de una tarea."""
    tareas = cargar_tareas()
    try:
        tareas[indice]["titulo"] = nuevo_titulo
        guardar_tareas(tareas)
        print(f"Tarea {indice + 1} actualizada a '{nuevo_titulo}'.")
    except IndexError:
        print("Error: Número de tarea no válido.")


def eliminar_tarea(indice):
    """Elimina una tarea."""
    tareas = cargar_tareas()
    try:
        tarea_eliminada = tareas.pop(indice)
        guardar_tareas(tareas)
        print(f"Tarea '{tarea_eliminada['titulo']}' eliminada.")
    except IndexError:
        print("Error: Número de tarea no válido.")


def cambiar_estado(indice, estado):
    """Cambia el estado de una tarea."""
    tareas = cargar_tareas()
    try:
        tareas[indice]["estado"] = estado
        guardar_tareas(tareas)
        print(f"Tarea {indice + 1} marcada como '{estado}'.")
    except IndexError:
        print("Error: Número de tarea no válido.")


def main():
    if len(sys.argv) < 2:
        print("Uso: python task_tracker.py <comando> [opciones]")
        return

    comando = sys.argv[1].lower()

    if comando == "listar":
        estado = sys.argv[2].lower() if len(sys.argv) > 2 else None
        listar_tareas(estado_filtro=estado)
    elif comando == "agregar":
        if len(sys.argv) < 3:
            print("Uso: python task_tracker.py agregar <título>")
        else:
            agregar_tarea(" ".join(sys.argv[2:]))
    elif comando == "actualizar":
        if len(sys.argv) < 4:
            print("Uso: python task_tracker.py actualizar <número_tarea> <nuevo_título>")
        else:
            try:
                indice = int(sys.argv[2]) - 1
                actualizar_tarea(indice, " ".join(sys.argv[3:]))
            except ValueError:
                print("Error: El número de tarea debe ser un entero.")
    elif comando == "eliminar":
        if len(sys.argv) < 3:
            print("Uso: python task_tracker.py eliminar <número_tarea>")
        else:
            try:
                indice = int(sys.argv[2]) - 1
                eliminar_tarea(indice)
            except ValueError:
                print("Error: El número de tarea debe ser un entero.")
    elif comando == "marcar":
        if len(sys.argv) < 4:
            print("Uso: python task_tracker.py marcar <número_tarea> <estado>")
        else:
            try:
                indice = int(sys.argv[2]) - 1
                estado = sys.argv[3].lower()
                if estado not in ["pendiente", "en progreso", "completada"]:
                    print("Error: El estado debe ser 'pendiente', 'en progreso' o 'completada'.")
                else:
                    cambiar_estado(indice, estado)
            except ValueError:
                print("Error: El número de tarea debe ser un entero.")
    else:
        print("Comando no válido. Comandos disponibles: listar, agregar, actualizar, eliminar, marcar")


if __name__ == "__main__":
    main()