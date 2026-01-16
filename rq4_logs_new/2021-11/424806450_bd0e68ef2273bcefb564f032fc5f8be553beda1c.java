/**
 * Clase Vendedor 
 * Clase que muestra los datos de un vendedor de autos de una agencia 
 * @author Brenda Paola Resendiz Mendoza 
 * @author Dulce Julieta Mora Hernandez 
 * @version 1.0 
 */

public class Vendedor{

    // Atributos
    private String nombre;
    private int codigodelvendedor;
    private String genero;
    private int añodeingreso;
    private String sede;
    private int autosvendidos;

    /**
     * Constructor que recibe 6 valores, 3 string y 3 int
     *
     * @param nombre Representa el nombre del vendedor 
     * @param genero Representa el genero del vendedor 
     * @param sede Representa la sede en donde trabaja el vendedor 
     *
     * @param codigodelvendedor Representa el codigo del vendedor 
     * @param añodeingreso Representa el año en que ingreso a trabajar el vendedor
     * @param autosvendidos Representa la cantidad de autos vendidos 
     */

    public Vendedor(String nombre, int codigodelvendedor, String genero, int añodeingreso, String sede, int autosvendidos)
    {
	this.nombre = nombre;
	this.codigodelvendedor = codigodelvendedor;
	this.genero = genero;
	this.añodeingreso = añodeingreso;
	this.sede = sede;
	this.autosvendidos = autosvendidos;
    }

    /**
     * Metodos get y set del atributo nombre
     */
    public String getNombre() {
	return nombre;
    }
    
    public void setNombre(String nombre) {
	this.nombre = nombre;

    }

     /**
     * Metodos get y set del atributo codigo del vendedor 
     */

    public int getCodigodelvendedor() {
	return codigodelvendedor;
    }
    
    public void setCodigodelvendedor(int codigodelvendeor) {
	this.codigodelvendedor = codigodelvendedor;

    }

     /**
     * Metodos get y set del atributo genero
     */

    public String getGenero() {
	return genero;
    }
    
    public void setGenero(String genero) {
	this.genero = genero;

    }

     /**
     * Metodos get y set del atributo año de ingreso
     */

     public int getAñodeingreso() {
	return añodeingreso;
    }
    
    public void setAñodeingreso(int añodeingreso) {
	this.añodeingreso = añodeingreso;

    }

     /**
     * Metodos get y set del atributo sede
     */

    public String getSede() {
	return sede;
    }
    
    public void setSede(String sede) {
	this.sede = sede;

    }

     /**
     * Metodos get y set del atributo autos vendidos 
     */

     public int getAutosvendidos() {
	return autosvendidos;
    }
    
    public void setAutosvendidos(String autosvendidos) {
	this.autosvendidos = autosvendidos;

    }

    
}
