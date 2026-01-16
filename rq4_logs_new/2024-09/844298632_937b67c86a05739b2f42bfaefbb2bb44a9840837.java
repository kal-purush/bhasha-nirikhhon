public class ListaDoble {
    private NodoDoble inicio, fin; // Nodo de inicio y Nodo final ...

    // Constructor ListaDoble
    // Se define como una ListaDoble vacía ...
    public ListaDoble(){
        inicio = null;
        fin = null;
    }

    // Método para determinar si la lista está vacía ...
    public boolean estaVacia(){
        return inicio == null; // Retorna true si el inicio de la listá está vacía ...
    }

    // Método para agregar nodos al final ...
    public void agregarNodoFinal(int elem){
        if(estaVacia()){
            inicio = fin = new NodoDoble(elem);
        }else{
            fin = new NodoDoble(elem, null, fin);
            fin.anterior.siguiente = fin;
        }
    }

    // Método para agregar al inicio ...
    public void agregarNodoInicio(int elem){
        if(estaVacia()){
            inicio = fin = new NodoDoble(elem);
        }else{
            inicio = new NodoDoble(elem, inicio, null);
            inicio.siguiente.anterior = inicio;
        }
    }

    // Método para mostrar la lista INICIO - FIN
    public void mostrarListaInicioFin(){
        if(!estaVacia()){
            NodoDoble aux = inicio;
            
            while(aux != null){
                System.out.print(" (" + aux.dato + ") < = >");
                aux = aux.siguiente;                                        
            }
        }else{
            System.out.println("LISTA VACÍA ...");
        }
    }

    // Método para mostrar la lista FIN - INICIO
    public void mostrarListaFinInicio(){
        if(!estaVacia()){
            NodoDoble aux = fin;
            
            while(aux != null){
                System.out.print(" (" + aux.dato + ") < = >");
                aux = aux.anterior;                                        
            }
        }else{
            System.out.println("LISTA VACÍA ...");
        }
    }


}