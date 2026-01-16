
public class Lista_Doblemente {
    private Nodo cabeza;
    private Nodo fin;

    public Lista_Doblemente() {
        cabeza = null;
        fin = null;
    }

    public boolean isEmpty() {
        return cabeza == null;
    }

    public void agregarInicioList(int valor) {
        Nodo nuevoNodo = new Nodo(valor);
        if (isEmpty()) {
            cabeza = fin = nuevoNodo;
        } else {
            nuevoNodo.next = cabeza;
            cabeza.prev = nuevoNodo;
            cabeza = nuevoNodo;

        }
    }

    public void agregarFinList(int valor) {
        Nodo nuevoNodo = new Nodo(valor);
        if (isEmpty()) {
            cabeza = fin = nuevoNodo;
        } else {
            fin.next = nuevoNodo;
            nuevoNodo.prev = fin;
            fin = nuevoNodo;

        }
    }

    public void mostrarInicioFin() {
        if (isEmpty()) {
            System.out.println("la lista esta vacia");
        }
        Nodo actual = cabeza;
        while (actual != null) {
            System.out.print(actual.valor + " <-> ");
            actual = actual.next;
        }
        System.out.println("null");
    }

    public void mostrarFinInicio() {
        if (isEmpty()) {
            System.out.println("la lista esta vacia");
        }
        Nodo actual = fin;
        while (actual != null) {
            System.out.print(actual.valor + " <-> ");
            actual = actual.prev;
        }
        System.out.println("null");
    }
}