
import javax.swing.JOptionPane;

public class Lista_Doble_Enlazada {
    Nodos cabeza;
    Nodos cola;
    
    public Lista_Doble_Enlazada(){
        this.cabeza = null;
        this.cola = null;
    }
    
    //saber si la lista esta vacia
    public boolean EsVacia(){
        return cabeza==null;
    }
    
    //agregar nodos al final
    public void Agregar_Nodo_Final(int Elemento){
        if(!EsVacia()){
            cola = new Nodos(Elemento, null, cola);
            cola.Nodo_Anterior.Nodo_Siguiente = cola;
        }else{
            cabeza = cola = new Nodos(Elemento);
        }
    }
    
    //agregar nodos al inicio
    public void Agregar_Nodo_Inicio(int Elemento){
        if(!EsVacia()){
            cabeza = new Nodos(Elemento, cabeza, null);
            cabeza.Nodo_Siguiente.Nodo_Anterior = cabeza;
        }else{
            cabeza=cola=new Nodos(Elemento);
        }
    }
    
    //Mostrar la lista de inicio a fin
    public void Mostar_Lista_Orden(){
        if(!EsVacia()){
            String datos="<=>";
            Nodos auxiliar = cabeza;
            while(auxiliar != null){
                datos = datos + "["+auxiliar.Valor+"]<=>";
                auxiliar = auxiliar.Nodo_Siguiente;
            }
            JOptionPane.showMessageDialog(null, datos,
                    "Mostrando lista de inicio a fin",
                    JOptionPane.INFORMATION_MESSAGE);
        }
    }
    
    //Mostrar la lista de fin a inicio
    public void Mostar_Lista_SinOrden(){
        if(!EsVacia()){
            String datos="<=>";
            Nodos auxiliar = cola;
            while(auxiliar != null){
                datos = datos + "["+auxiliar.Valor+"]<=>";
                auxiliar = auxiliar.Nodo_Anterior;
            }
            JOptionPane.showMessageDialog(null, datos,
                    "Mostrando lista de inicio a fin",
                    JOptionPane.INFORMATION_MESSAGE);
        }
    }
    
    //Metodo para eliminar del inicio
    public int Eliminar_Nodo_Inicio(){
        int Elemento = cabeza.Valor;
        if(cabeza == cola){
            cabeza = cola = null;
        }else{
            cabeza = cabeza.Nodo_Siguiente;
            cabeza.Nodo_Anterior = null;
        }
        return Elemento;  
    }
    
    ////Metodo para eliminar del final
    public int Eliminar_Nodo_Final(){
        int Elemento = cola.Valor;
        if(cabeza == cola){
            cabeza = cola = null;
        }else{
            cola = cola.Nodo_Anterior;
            cola.Nodo_Siguiente = null;
        }
        return Elemento;  
    }
}