/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package listasdoblementeligadas;

/**
 *
 * @author HP
 */
public class ListaDoblementeLigada<T> {

    private NodoDoble<T> head;
    int tamanio = 0;

    public ListaDoblementeLigada() {
        this.head = null;
        this.tamanio = 0;
    }

    public boolean estaVacia() {
        return this.head == null;

    }

    public int getTamanio() {
        int cont = 0;
        NodoDoble aux = this.head;
        while (aux != null) {
            cont += 1;
            aux = aux.getSiguiente();
        }
        return cont;

    }

    public void agregarAlInicio(T valor) {
        NodoDoble nuevo = new NodoDoble(valor);
        NodoDoble aux = this.head;
        if (this.estaVacia()) {
            this.head = nuevo;
        } else {
            nuevo.setSiguiente(aux);
            aux.setAnterior(nuevo);
            this.head = nuevo;
        }

    }

    public void agregarAlFinal(T valor) {
        NodoDoble nuevo = new NodoDoble(valor);
        NodoDoble aux = this.head;
        if (this.estaVacia()) {
            this.head = nuevo;
        } else {
            while (aux.getSiguiente() != null) {
                aux = aux.getSiguiente();
            }
            nuevo.setAnterior(aux);
            aux.setSiguiente(nuevo);

        }
    }

    public void agregarDespuesDe(int posicion, T valor) {
        NodoDoble nuevo = new NodoDoble(valor);
        NodoDoble aux = this.head;
        if (posicion >= 0 && posicion <= this.getTamanio()) {
            if (this.estaVacia() || posicion == 0) {
                this.agregarAlInicio(valor);
            } else if (posicion == this.getTamanio()) {
                this.agregarAlFinal(valor);
            } else {
                for (int i = 1; i < posicion; i++) {
                    aux = aux.getSiguiente();
                }
                aux.getSiguiente().setAnterior(nuevo);
                nuevo.setSiguiente(aux.getSiguiente());
                nuevo.setAnterior(aux);
                aux.setSiguiente(nuevo);
            }
        } else {
            System.out.println("La posición no existe en la lista");
        }
    }

    public void eliminarPrimero() {
        NodoDoble aux = this.head;
        System.out.println("\nBorrando " + head.getDato());
        this.head = aux.getSiguiente();
        head.setAnterior(null);
    }

    public void eliminarFinal() {
        NodoDoble aux = this.head;
        while (aux.getSiguiente() != null) {
            aux = aux.getSiguiente();
        }
        System.out.println("\nBorrando " + aux.getDato());
        aux = aux.getAnterior();
        aux.setSiguiente(null);
    }

    public void eliminarPosicion(int posicion) {
        NodoDoble curr = this.head;
        if (posicion > 0 && posicion <= this.getTamanio()) {
            if (this.estaVacia()) {
                System.out.println("Lista Vacía, nada por borrar");
            } else if (posicion == 1) {
                this.eliminarPrimero();
            } else if (posicion == this.getTamanio()) {
                this.eliminarFinal();
            } else {
                for (int i = 1; i < posicion - 1; i++) {
                    curr = curr.getSiguiente();
                }
                System.out.println("\nBorrando " + curr.getSiguiente().getDato());
                curr.getSiguiente().getSiguiente().setAnterior(curr);
                curr.setSiguiente(curr.getSiguiente().getSiguiente());
            }
        } else {
            System.out.println("La posicion no existe en la lista");
        }
    }

    public int buscar(T valor) {
        NodoDoble aux = this.head;
        boolean encontrado = false;
        for (int i = 1; i <= this.getTamanio(); i++) {
            if (aux.getDato() == valor) {
                encontrado = true;
                return i;
            } else {
                aux = aux.getSiguiente();
            }
        }
        if (encontrado == false) {
            System.out.println("No se encontró el valor RETURN 0");
        }
        return 0;
    }
    
    public void buscarVarios(T valor){
      
    NodoDoble aux = this.head;
        boolean encontrado = false;
        for (int i = 1; i <= this.getTamanio(); i++) {
            if (aux.getDato() == valor) {
                //System.out.println("mmmmm"+i);
                encontrado = true;
                System.out.println("\nEncontrado en el lugar: "+i);
                aux = aux.getSiguiente();
            } else {
                //System.out.println("mmmmm"+i);
                aux = aux.getSiguiente();
            }
        }
        if (encontrado == false) {
            System.out.println("No se encontró el valor "+ valor);
        }
        
    
    }
    
    public void actualizar(T buscar, T reemplazar){
        NodoDoble curr = this.head;
        Boolean encontrado = false;
        System.out.println("\n Reemplazando " + buscar + " Por " + reemplazar + "...");
        for (int i = 1; i <= this.getTamanio(); i++) {
            if (curr.getDato() == buscar) {
                encontrado = true;
                System.out.println("\nVALOR " + curr.getDato() + " REMPLAZADO POR " + reemplazar + "  c:");
                curr.setDato(reemplazar);

                curr = curr.getSiguiente();
            } else {
                curr = curr.getSiguiente();
            }
        }
        if (encontrado == false) {

            System.out.println("NADA POR REEMPLAZAR  :c");
        }

    }

    public void transversal() {
        NodoDoble aux = this.head;
        while (aux != null) {
            System.out.print("--" + aux.getDato() + "--");
            aux = aux.getSiguiente();
        }
        System.out.println("");
    }

    public static void main(String[] args) {
        ListaDoblementeLigada list = new ListaDoblementeLigada();

        list.agregarAlInicio(30);
        list.agregarAlInicio(40);
        list.agregarAlInicio(50);
        list.agregarAlInicio(60);
        list.transversal();
        list.agregarAlFinal(20);
        list.transversal();
        list.agregarDespuesDe(5, 10);
        list.transversal();
        list.agregarDespuesDe(0, 70);
        list.transversal();
        list.eliminarPrimero();
        list.transversal();
        list.eliminarFinal();
        list.transversal();
        list.eliminarPosicion(3);
        list.transversal();
        list.agregarAlFinal(10);
        list.agregarAlInicio(70);
        list.agregarAlInicio(80);
        list.agregarDespuesDe(4, 40);
        list.transversal();
        list.eliminarPosicion(2);
        list.transversal();
        System.out.println(list.buscar(50));
        list.agregarDespuesDe(1, 70);
        list.agregarDespuesDe(1, 100);
        //list.agregarDespuesDe(3, 501);
        //list.transversal();
        //list.buscarVarios(50);
        //list.actualizar(50, 500);
        list.transversal();
        //list.buscarVarios(100);
        System.out.println(list.buscar(100));
        list.actualizar(100, 75);
        list.transversal();
        System.out.println("\nTAMAÑO " + list.getTamanio());
    }

}