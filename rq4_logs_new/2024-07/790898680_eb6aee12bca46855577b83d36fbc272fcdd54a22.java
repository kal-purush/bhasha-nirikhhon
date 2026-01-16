package Backend.Models;

import Backend.DB.DB;
import java.nio.file.Path;
import java.util.ArrayList;
<<<<<<< Updated upstream

import Backend.DB.DB;
=======
>>>>>>> Stashed changes
/**
 * Clase refugio hereda de Usuario porque es un tipo de usuario.
 */
public class Refugio extends Usuario{
    /*
     * Atributos
     */
    private String direccion;
    ArrayList<String> tipo_mascota;
    /*
     * Metodos
     */
    public Refugio(){}
    /**
     * Constructor que amplia el constructor de usuario con los atributos de esta clase.
     * @param nombre
     * @param contraseña
     * @param numero_contacto
     * @param direccion
     * @param tipo_mascota
     * @param foto
     */
    public Refugio(int id, String nombre, String contraseña, String numero_contacto, String direccion, ArrayList<String> tipo_mascota, Path foto) {
        super(id,nombre, contraseña, numero_contacto,foto);
        setAcceso(true); // True indica que el usuario es un refugio.
        this.direccion = direccion;
        this.tipo_mascota = tipo_mascota;
    }

<<<<<<< Updated upstream
    public boolean registrarse(String nombre, String contra, String numero, String direccion, DB db){
        if(db.getUsername(nombre) == null){
=======
    public boolean registrarse(String nombre, String contra, String numero, String direccion, DB db) {
        if(DB.getUsuario(nombre) == null){
>>>>>>> Stashed changes
            setNombre(nombre);
            setContraseña(contra);
            setNumero_contacto(numero);
            setDireccion(direccion);
            setAcceso(true);
            return true;
        }
        System.out.println("No se pudo registrar el usuario.");
        return false;
    }

    public String getDireccion() {
        return direccion;
    }

    public void setDireccion(String direccion) {
        this.direccion = direccion;
    }

    public ArrayList<String> getTipo_mascota() {
        return tipo_mascota;
    }

    public void setTipo_mascota(ArrayList<String> tipo_mascota) {
        this.tipo_mascota = tipo_mascota;
    }
    
}