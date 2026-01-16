package clases;

import java.util.ArrayList;
import java.util.List;

public class Usuario {

    private String nombre;
    private String userName;
    private String contrasenia;
    private List<List> Listas = new ArrayList<>();

    public Usuario(String nombre, String userName, String contrasenia){
        this.nombre = nombre;
        this.userName = userName;
        this.contrasenia = contrasenia;
    }

    public List<List> getListas() {
        return Listas;
    }

    public void setListas(List<List> listas) {
        Listas = listas;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getContrasenia() {
        return contrasenia;
    }

    public void setContrasenia(String contrasenia) {
        this.contrasenia = contrasenia;
    }
}