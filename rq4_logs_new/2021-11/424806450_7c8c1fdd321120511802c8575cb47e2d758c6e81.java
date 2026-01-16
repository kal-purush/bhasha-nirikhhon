/**
 * Clase que representa un Cliente 
 * @author Ethan Damian
 * @version 0.1 // 8-nov-2021
*/

public class Cliente{

    //El nombre cliente
    private String nombre;

    //El dinero dispobible del cliente
    private int dineroDisponible;


    //CONSTRUCTORES

    /**
     * Constructor que recibe dos argumentos
     * @param nombre Nombre del cliente
     * @param dineroDisponible Es el dinero disponible del cliente
     */

    public Cliente(String nombre, int dineroDisponible){
	this.nombre = nombre;
	this.dineroDisponible = dineroDisponible;
    }


    //METODOS

    //GETTERS

    /**
     * Metodo que devuelve el nombre del cliente 
     * @return El nombre del cliente 
     */

    public String getNombre(){

	return this.nombre;
    }

    /**
     * Metodo que devuelve el dinero disponible del cliente 
     * @return El dinero disponible del cliente 
     */

    public int dineroDisponible(){

	return this.dineroDisponible;
    }

    
    //SETTERS
    
    /**
     * Metodo que le asigna un nuevo nombre del cliente  
     * @param nombre El nuevo nombre del cliente
     */

    public void setNombre(String nombre){
	this.nombre = nombre;
    }

    /**
     * Metodo que le asigna un nuevo valor al dinero disponible del cliente  
     * @param dineroDisponible El dinero que ahora dispone del cliente
     */

    public void setNombre(int dineroDisponible){
	this.dineroDisponible = dineroDisponible;
    }


    //METODOS ESPECIALES
    
    /**
     * Metodo toString que devuelve un objeto en forma de cadena 
     * @return El objeto en forma de cadena  
    */

    public String toString(){
	return "Nombre del cliente: " + this.nombre + "\n" + "Dinero disponible: " + this.dineroDisponible; 
    }
    

}//fin de la clase Cliente