/*
 * Clase que representa a un automóvil
 * @author Isaac Alcantara y Eduardo Vargas
 * @version 1.0
 * */

public class Auto{
    //Atributos

    //Marca del auto
    private String marca;
    //Modelo (nombre) del auto
    private String modelo;
    //Año de fabricación
    private int ano;
    //Precio del auto
    private double precio;
    //Color del auto
    private String color;

    //Métodos
    //Métodos Constructores
    /*
     * Constructor sin parametros, crea un automóvil de la marca Toyota, 
     * modelo Prius, del año 2021 y color rojo
     * 
     * */
    public Auto(){
	this.marca = Toyota;
	this.modelo = Prius;
	this.ano = 2021;
	this.color = Rojo;
    }
    
    //Métodos Observadores
    /*
     * Método que devuelve la marca del automóvil
     *
     * @return La marca del auto
     * */
    public String getMarca(){
	return this.marca;
    }

    /*
     * Método que devuelve el modelo del auto (nombre)
     *
     * @return El nombre del modelo del auto
     * */
    public String getModelo(){
	return this.modelo;
    }

    /*
     * Método que devuelve el año de fabricación del automóvil
     *
     * @return El año de fabricación del auto.
     * */
    public int getAno(){
	return this.ano;
    }

    /*
     * Método que devuelve el precio del automóvil
     *
     * @return El precio del auto
     * */
    public double getPrecio(){
	return this.precio;
    }

    /*
     * Método que devuelve el color del automóvil
     *
     * @return El color del auto.
     * */
    public String getColor(){
	return this.color;
    }
    
}