/**
 * Clase que representa un Auto
 * @author Ethan Damian
 * @version version 1.0// 8-nov-2021
*/

public class Auto{
    //El modelo del auto
    private String modelo;
    
    //El color del auto
    private String color;

    //el modelo del auto
    private int noPuertas;

    //El año del modelo
    private String año;

    //CONSTRUCTORES

    /**
     * Constructor que recibe cuatro paramteros
     * @param modelo El modelo del auto 
     * @param color El color del auto
     * @param noPuertas El numero de puertas del auto 
     * @param año EL año del auto
     */

    public Auto(String modelo, String color, int noPuertas, String año){
	this.modelo = modelo;
	this.color = color;
	this.noPuertas = noPuertas;
	this.año = año;
    } 

    //METODOS
    
    //GETTERS

    /**
     * Metodo que devuelve el modelo del auto 
     * @return El modelo del auto 
     */

    public String getModelo(){
	return this.modelo; 

    }

    
    /**
     * Metodo que devuelve el color del auto 
     * @return El color del auto 
     */

    public String getColor(){
	return this.color; 

    }

    
    /**
     * Metodo que devuelve el numero de puertas del auto 
     * @return El numero de puertas del auto 
     */

    public int getNoPuertas(){
	return this.noPuertas; 

    }

    
    /**
     * Metodo que devuelve el año del auto 
     * @return El año del auto 
     */

    public String getAño(){
	return this.año; 

    }

    //SETTERS
    /**
     * Metodo que cambia el modelo del auto
     * @param modelo El nuevo modelo del auto  
     */

    public void setModelo(String modelo){
	this.modelo = modelo;
    }

    
   
    /**
     * Metodo que cambia el color del auto
     * @param color El nuevo color del auto  
     */

    public void setColor(String color){
	this.color = color;
    }

    
   
    /**
     * Metodo que cambia el numero de puertas del auto
     * @param noPuertas El nuevo numero de puertas del auto  
     */

    public void setNoPuertas(int noPuertas){
	this.noPuertas = noPuertas;
    }

    
    /**
     * Metodo que cambia el año del auto
     * @param año El nuevo año del auto  
     */

    public void setAño(String año){
	this.año = año;
    }

    //METODOS EPECIALES

    /**
     * Metodo toString que regresa el objeto en forma de cadena 
     * @return El objeto en forma de cadena
     */

    public String toString(){
	return "Modelo : " + this.modelo +"\n" + "Color : " + this.color + "\n" + "No. de puertas : " + this.noPuertas + "\n" + "Año: " + this.año;
	
    }


    



    
    


}//fin de la clase autos