package Creacion_de_Clase;
/**
 * Clase casa que determina ciertas caracterista de ella
 * @author NEVARDO ANTONIO OSPINA ZUÑIGA
 */
public class Casa {
    //Atributos de la clase
    /**
     * Color de la casa
     */
    String color;
    /**
     * medida de la casa
     */
    float medida;
    /**
     * Direccion de la casa
     */
    protected String Direccion;
    
    //Metodo de la clase
    /**
     * Devuelve el color de la casa
     * @return 
     */

    public String getColor() {
        return color;
    }
    /**
     * Modificador del atributo Medida
     * @param medida 
     */
    public void setMedida(float medida) {
        this.medida = medida;
    }
    //Metodo protegido
    /**
     * Metodo para que devuelva el valor del atributo Direccion
     * @return 
     */
    protected String getDireccion() {
        return Direccion;
    }
    /**
     * Metodo para modificar el atributo direccion
     * @param Direccion 
     */
    protected void setDireccion(String Direccion) {
        this.Direccion = Direccion;
    }

}