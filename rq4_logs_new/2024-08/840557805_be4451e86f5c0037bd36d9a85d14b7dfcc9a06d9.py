import csv
import os

class Tarea:
    def _init_(self, id, descripcion, prioridad, categoria="General"):
        self.id = id 
        self.descripcion = descripcion 
        self.prioridad = prioridad  # 1 = baja, 2 = media, 3 = alta
        self.completada = False 
        self.categoria = categoria 

class Nodo:
    def _init_(self, tarea):
        self.tarea = tarea  # Objeto tarea asociado al nodo
        self.siguiente = None  # Enlace al siguiente nodo en la lista

class ListaEnlazada:
    def _init_(self):
        self.cabeza = None  # Cabeza de la lista (inicio)
        self.id_actual = 1  # Contador para asignar ID único a nuevas tareas

    def esta_vacia(self):
        return self.cabeza is None  # Devuelve True si la lista está vacía

    def agregar_tarea(self, descripcion, prioridad, categoria):
        # Crea una nueva tarea con un ID único
        tarea = Tarea(self.id_actual, descripcion, prioridad, categoria)
        nuevo_nodo = Nodo(tarea)
        self.id_actual += 1

        # Inserta la nueva tarea en la lista ordenada por prioridad
        if self.esta_vacia() or tarea.prioridad > self.cabeza.tarea.prioridad:
            nuevo_nodo.siguiente = self.cabeza
            self.cabeza = nuevo_nodo
        else:
            actual = self.cabeza
            while actual.siguiente is not None and actual.siguiente.tarea.prioridad >= tarea.prioridad:
                actual = actual.siguiente
            nuevo_nodo.siguiente = actual.siguiente
            actual.siguiente = nuevo_nodo

        print("Tarea agregada con éxito.")

    def buscar_tarea_descripcion(self, texto):
        # Busca tareas por descripción y las muestra
        actual = self.cabeza
        encontrado = False
        while actual is not None:
            if texto.lower() in actual.tarea.descripcion.lower():
                print(f"ID: {actual.tarea.id}, Descripción: {actual.tarea.descripcion}, Prioridad: {actual.tarea.prioridad}, Categoría: {actual.tarea.categoria}, Estado: {'Completada' if actual.tarea.completada else 'Pendiente'}")
                encontrado = True
            actual = actual.siguiente
        if not encontrado:
            print(f"No se encontraron tareas con el texto '{texto}'.")

    def completar_tarea(self, id):
        # Marca una tarea como completada
        actual = self.cabeza
        while actual is not None:
            if actual.tarea.id == id:
                actual.tarea.completada = True
                print(f"Tarea completada: {actual.tarea.descripcion}")
                return
            actual = actual.siguiente
        print(f"Tarea con ID {id} no encontrada.")

    def eliminar_tarea(self, id):
        # Elimina una tarea de la lista
        actual = self.cabeza
        previo = None
        while actual is not None:
            if actual.tarea.id == id:
                if previo is None:
                    self.cabeza = actual.siguiente
                else:
                    previo.siguiente = actual.siguiente
                print(f"Tarea eliminada: {actual.tarea.descripcion}")
                return
            previo = actual
            actual = actual.siguiente
        print(f"Tarea con ID {id} no encontrada.")

    def mostrar_tareas(self):
        # Muestra todas las tareas
        actual = self.cabeza
        while actual is not None:
            estado = "Completada" if actual.tarea.completada else "Pendiente"
            print(f"ID: {actual.tarea.id}, Descripción: {actual.tarea.descripcion}, Prioridad: {actual.tarea.prioridad}, Categoría: {actual.tarea.categoria}, Estado: {estado}")
            actual = actual.siguiente

    def mostrar_tareas_pendientes(self):
        # Muestra solo las tareas pendientes
        actual = self.cabeza
        while actual is not None:
            if not actual.tarea.completada:
                print(f"ID: {actual.tarea.id}, Descripción: {actual.tarea.descripcion}, Prioridad: {actual.tarea.prioridad}, Categoría: {actual.tarea.categoria}")
            actual = actual.siguiente

    def mostrar_tareas_categoria(self, categoria):
        # Muestra tareas según la categoría especificada
        actual = self.cabeza
        encontrado = False
        while actual is not None:
            if actual.tarea.categoria.lower() == categoria.lower():
                print(f"ID: {actual.tarea.id}, Descripción: {actual.tarea.descripcion}, Prioridad: {actual.tarea.prioridad}, Estado: {'Completada' if actual.tarea.completada else 'Pendiente'}")
                encontrado = True
            actual = actual.siguiente
        if not encontrado:
            print(f"No se encontraron tareas en la categoría '{categoria}'.")

    def contar_tareas_pendientes(self):
        # Cuenta el número de tareas pendientes
        contador = 0
        actual = self.cabeza
        while actual is not None:
            if not actual.tarea.completada:
                contador += 1
            actual = actual.siguiente
        return contador

    def mostrar_estadisticas(self):
        # Muestra estadísticas sobre las tareas
        total_tareas = 0
        tareas_pendientes = 0
        actual = self.cabeza
        while actual is not None:
            total_tareas += 1
            if not actual.tarea.completada:
                tareas_pendientes += 1
            actual = actual.siguiente
        print(f"Total de tareas: {total_tareas}")
        print(f"Tareas pendientes: {tareas_pendientes}")

    def guardar_en_csv(self, archivo):
        # Guarda las tareas en un archivo CSV
        with open(archivo, mode='w', newline='') as file:
            writer = csv.writer(file)
            actual = self.cabeza
            while actual is not None:
                writer.writerow([actual.tarea.id, actual.tarea.descripcion, actual.tarea.prioridad, actual.tarea.categoria, actual.tarea.completada])
                actual = actual.siguiente
        print(f"Tareas guardadas en {archivo} con éxito.")

    def cargar_desde_csv(self, archivo):
        # Carga las tareas desde un archivo CSV
        if not os.path.exists(archivo):
            print(f"Archivo {archivo} no encontrado.")
            return
        with open(archivo, mode='r') as file:
            reader = csv.reader(file)
            for row in reader:
                id, descripcion, prioridad, categoria, completada = int(row[0]), row[1], int(row[2]), row[3], row[4] == 'True'
                tarea = Tarea(id, descripcion, prioridad, categoria)
                tarea.completada = completada
                self.agregar_tarea_existente(tarea)
            print(f"Tareas cargadas desde {archivo} con éxito.")

    def agregar_tarea_existente(self, tarea):
        # Agrega tarea existente
        nuevo_nodo = Nodo(tarea)
        if self.esta_vacia() or tarea.prioridad > self.cabeza.tarea.prioridad:
            nuevo_nodo.siguiente = self.cabeza
            self.cabeza = nuevo_nodo
        else:
            actual = self.cabeza
            while actual.siguiente is not None and actual.siguiente.tarea.prioridad >= tarea.prioridad:
                actual = actual.siguiente
            nuevo_nodo.siguiente = actual.siguiente
            actual.siguiente = nuevo_nodo

        if tarea.id >= self.id_actual:
            self.id_actual = tarea.id + 1

# Menú
def menu():
    print("\nMenú:")
    print("1. Agregar tarea")
    print("2. Completar tarea")
    print("3. Eliminar tarea")
    print("4. Mostrar todas las tareas")
    print("5. Mostrar tareas pendientes")
    print("6. Guardar tareas en archivo CSV")
    print("7. Cargar tareas desde archivo CSV")
    print("8. Salir")

def main():
    lista_tareas = ListaEnlazada()
    archivo_csv = 'tareas.csv'

    # Cargar tareas desde el CSV
    lista_tareas.cargar_desde_csv(archivo_csv)

    while True:
        menu()
        opcion = input("Seleccione una opción: ")
        if opcion == "1":
            descripcion = input("Ingrese la descripción de la tarea: ")
            prioridad = int(input("Ingrese la prioridad de la tarea (1 = baja, 2 = media, 3 = alta): "))
            categoria = input("Ingrese la categoría de la tarea: ")
            lista_tareas.agregar_tarea(descripcion, prioridad, categoria)
        elif opcion == "2":
            id_tarea = int(input("Ingrese el ID de la tarea a completar: "))
            lista_tareas.completar_tarea(id_tarea)
        elif opcion == "3":
            id_tarea = int(input("Ingrese el ID de la tarea a eliminar: "))
            lista_tareas.eliminar_tarea(id_tarea)
        elif opcion == "4":
            lista_tareas.mostrar_tareas()
        elif opcion == "5":
            lista_tareas.mostrar_tareas_pendientes()
        elif opcion == "6":
            lista_tareas.guardar_en_csv(archivo_csv)
        elif opcion == "7":
            lista_tareas.cargar_desde_csv(archivo_csv)
        elif opcion == "8":
            print("Saliendo del sistema de gestión de tareas.")
            break
        else:
            print("Opción no válida. Por favor, seleccione una opción válida.")

if _name_ == "_main_":
    main()