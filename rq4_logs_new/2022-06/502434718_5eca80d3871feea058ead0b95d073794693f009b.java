package coloreoDeGrafos_matAdy;

import java.util.PriorityQueue;

public class Grafo {
	private int cantNodos;
	private int[][] matAdy;
	private int[] colores;
	private int[] nivel;

	public Grafo(int cantNodos) {
		this.cantNodos = cantNodos;
		matAdy = new int[cantNodos + 1][cantNodos + 1];
		colores = new int[cantNodos + 1];
		nivel = new int[cantNodos + 1];
	}

	public void setArista(int nodo1, int nodo2) {
		matAdy[nodo1][nodo2] = 1;
		matAdy[nodo2][nodo1] = 1;
		nivel[nodo1]++;
		nivel[nodo2]++;
	}

	public void colorearGrafo() {
		PriorityQueue<Nodo> cola = new PriorityQueue<Nodo>();
		for (int i = 1; i <= cantNodos; i++) {
			cola.add(new Nodo(i, nivel[i]));
		}

		while (!cola.isEmpty()) {
			Nodo actual = cola.poll();
			int color = 1;
			boolean estaColoreado = false;
			while (!estaColoreado) {
				boolean colorDisponible = true;
				int recorrer = 1;
				while (colorDisponible && recorrer <= cantNodos) {
					if (matAdy[actual.id][recorrer] == 1 && colores[recorrer] == color) {
						colorDisponible = false;
					}
					recorrer++;
				}
				if (colorDisponible) {
					colores[actual.id] = color;
					estaColoreado = true;
				} else {
					color++;
				}
			}
		}
	}

	public int[] getColores() {
		return colores;
	}

	public String mostrarColores() {
		String sal = "";

		int contadorDeNodos = 0;
		int color = 1;
		while (contadorDeNodos < cantNodos) {
			sal += "Color " + color + ": ";
			for (int i = 1; i <= cantNodos; i++) {
				if (colores[i] == color) {
					sal += i + " ";
					contadorDeNodos++;
				}
			}
			color++;
			sal+="\n";
		}

		return sal;
	}
}