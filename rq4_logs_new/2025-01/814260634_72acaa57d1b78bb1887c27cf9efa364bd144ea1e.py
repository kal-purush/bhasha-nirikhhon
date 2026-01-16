import networkx as nx
import matplotlib.pyplot as plt
from typing import Optional

class NodoListaEnlazada:
    """Nodo para listas enlazadas."""
    def __init__(self, valor):
        self.valor = valor
        self.siguiente: Optional[NodoListaEnlazada] = None

class ListaEnlazada:
    """Implementación de una lista enlazada simple."""
    def __init__(self):
        self.cabeza = None

    def agregar(self, valor):
        """Agrega un valor al final de la lista."""
        nuevo_nodo = NodoListaEnlazada(valor)
        if not self.cabeza:
            self.cabeza = nuevo_nodo
        else:
            actual = self.cabeza
            while actual.siguiente:
                actual = actual.siguiente
            actual.siguiente = nuevo_nodo

    def mostrar(self):
        """Muestra los elementos de la lista."""
        actual = self.cabeza
        elementos = []
        while actual:
            elementos.append(actual.valor)
            actual = actual.siguiente
        return elementos

class NodoABB:
    """Nodo para el Árbol Binario de Búsqueda."""
    def __init__(self, clave, datos):
        self.clave = clave
        self.datos = datos
        self.izquierda = None
        self.derecha = None

class ArbolBinarioBusqueda:
    """Implementación de un Árbol Binario de Búsqueda."""
    def __init__(self):
        self.raiz = None

    def insertar(self, clave, datos):
        """Inserta un nodo en el ABB."""
        if not self.raiz:
            self.raiz = NodoABB(clave, datos)
        else:
            self._insertar_recursivo(self.raiz, clave, datos)

    def _insertar_recursivo(self, nodo, clave, datos):
        if clave < nodo.clave:
            if nodo.izquierda:
                self._insertar_recursivo(nodo.izquierda, clave, datos)
            else:
                nodo.izquierda = NodoABB(clave, datos)
        elif clave > nodo.clave:
            if nodo.derecha:
                self._insertar_recursivo(nodo.derecha, clave, datos)
            else:
                nodo.derecha = NodoABB(clave, datos)

    def buscar(self, clave):
        """Busca un nodo en el ABB por su clave."""
        return self._buscar_recursivo(self.raiz, clave)

    def _buscar_recursivo(self, nodo, clave):
        if not nodo or nodo.clave == clave:
            return nodo
        elif clave < nodo.clave:
            return self._buscar_recursivo(nodo.izquierda, clave)
        else:
            return self._buscar_recursivo(nodo.derecha, clave)

def crear_red_social():
    """
    Crea un grafo para modelar una red social donde los usuarios son nodos
    y las relaciones son aristas.
    """
    red_social = nx.DiGraph()
    red_social.add_nodes_from([
        ("usuario_1", {"nombre": "Ana", "edad": 25}),
        ("usuario_2", {"nombre": "Carlos", "edad": 30}),
        ("usuario_3", {"nombre": "Lucía", "edad": 28}),
        ("usuario_4", {"nombre": "Mario", "edad": 22}),
    ])
    red_social.add_edges_from([
        ("usuario_1", "usuario_2", {"tipo": "amistad", "peso": 1}),
        ("usuario_2", "usuario_3", {"tipo": "seguimiento", "peso": 2}),
        ("usuario_3", "usuario_1", {"tipo": "amistad", "peso": 1}),
        ("usuario_4", "usuario_2", {"tipo": "seguimiento", "peso": 3}),
    ])
    return red_social

def visualizar_red_social(red_social):
    """
    Genera una representación visual del grafo usando Matplotlib.
    """
    plt.figure(figsize=(10, 8))
    pos = nx.spring_layout(red_social)
    nx.draw(red_social, pos, with_labels=True, node_color='skyblue', edge_color='gray',
            node_size=2000, font_size=10, font_color='black')
    labels = nx.get_edge_attributes(red_social, 'tipo')
    nx.draw_networkx_edge_labels(red_social, pos, edge_labels=labels)
    plt.title("Red Social - Usuarios y Relaciones")
    plt.show()

def encontrar_camino_mas_corto(red_social, origen, destino):
    """
    Encuentra el camino más corto entre dos usuarios en términos de peso.
    """
    try:
        camino = nx.shortest_path(red_social, source=origen, target=destino, weight='peso')
        return camino
    except nx.NetworkXNoPath:
        return None

def main():
    red_social = crear_red_social()

    # Listas enlazadas para almacenar conexiones de cada usuario
    conexiones = {usuario: ListaEnlazada() for usuario in red_social.nodes}
    for origen, destino, _ in red_social.edges(data=True):
        conexiones[origen].agregar(destino)

    # Mostrar las conexiones almacenadas en listas enlazadas
    print("Conexiones de cada usuario (listas enlazadas):")
    for usuario, lista in conexiones.items():
        print(f"{usuario}: {lista.mostrar()}")

    # Árbol Binario de Búsqueda para búsqueda de usuarios por nombre
    arbol_usuarios = ArbolBinarioBusqueda()
    for nodo, datos in red_social.nodes(data=True):
        arbol_usuarios.insertar(datos["nombre"], {"id": nodo, "edad": datos["edad"]})

    # Búsqueda de un usuario en el ABB
    nombre_a_buscar = "Carlos"
    print(f"\nBuscando al usuario con nombre '{nombre_a_buscar}' en el ABB:")
    resultado = arbol_usuarios.buscar(nombre_a_buscar)
    if resultado:
        print(f"Usuario encontrado: {resultado.datos}")
    else:
        print("Usuario no encontrado.")

    # Visualizar la red social
    visualizar_red_social(red_social)

    # Encontrar el camino más corto entre dos usuarios
    origen = "usuario_1"
    destino = "usuario_3"
    print(f"\nCamino más corto entre {origen} y {destino}:")
    camino = encontrar_camino_mas_corto(red_social, origen, destino)
    if camino:
        print(" -> ".join(camino))
    else:
        print("No hay camino disponible entre los usuarios especificados.")

if __name__ == "__main__":
    main()