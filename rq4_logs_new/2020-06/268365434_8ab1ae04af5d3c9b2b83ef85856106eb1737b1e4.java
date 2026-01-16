package ar.edu.unq.tsp.model;

public class Direccion {

    private String calle;
    private String altura;
    private String localidad;
    private String provincia;

    public Direccion() {  }

    public Direccion(String calle, String altura, String localidad, String provincia) {
        this.calle = calle;
        this.altura = altura;
        this.localidad = localidad;
        this.provincia = provincia;
    }

    public String getCalle() {
        return calle;
    }

    public void setCalle(String calle) {
        this.calle = calle;
    }

    public String getAltura() {
        return altura;
    }

    public void setAltura(String altura) {
        this.altura = altura;
    }

    public String getLocalidad() {
        return localidad;
    }

    public void setLocalidad(String localidad) {
        this.localidad = localidad;
    }

    public String getProvincia() {
        return provincia;
    }

    public void setProvincia(String provincia) {
        this.provincia = provincia;
    }

    @Override
    public String toString() {
        return calle + ' ' + altura + ','  + localidad + ','  + provincia;
    }
}