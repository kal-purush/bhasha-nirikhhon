package Entidades;

/**
 * Created by ocamp on 17/08/2016.
 */
public class Negocio {

    private String IdNegocio;
    private String Clave;
    private String Nombre;
    private String Correo;
    private String Logo;


    public Negocio() {
    }

    public Negocio(String clave, String idNegocio, String nombre, String correo, String logo) {
        Clave = clave;
        IdNegocio = idNegocio;
        Nombre = nombre;
        Correo = correo;
        Logo = logo;
    }

    public String getIdNegocio() {
        return IdNegocio;
    }

    public void setIdNegocio(String idNegocio) {
        IdNegocio = idNegocio;
    }

    public String getClave() {
        return Clave;
    }

    public void setClave(String clave) {
        Clave = clave;
    }

    public String getNombre() {
        return Nombre;
    }

    public void setNombre(String nombre) {
        Nombre = nombre;
    }

    public String getCorreo() {
        return Correo;
    }

    public void setCorreo(String correo) {
        Correo = correo;
    }

    public String getLogo() {
        return Logo;
    }

    public void setLogo(String logo) {
        Logo = logo;
    }
}