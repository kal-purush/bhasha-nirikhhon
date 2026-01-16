package Backend.Models;

import Backend.DB.DB;
<<<<<<< Updated upstream
=======
import java.nio.file.Path;
>>>>>>> Stashed changes

public class Usuario {
    /*
     * Atributos
     */
    
     private int id;
     private String nombre, contraseña, numero_contacto;
     private boolean acceso; // Dara un nivel de acceso dependiendo si es un usuario comun o un refugio.
     private Path foto;
     /*
      * Metodos
      */
     public Usuario(){}
      /**
       * Constructor con todos los atributos
       * @param id ID que se usa para identificar al usuario.
       * @param nombre Nombre que usaran para su usuario
       * @param contraseña Contraseña del usuario
       * @param numero_contacto Numero telefonico
<<<<<<< Updated upstream
       */
     public Usuario(int id, String nombre, String contraseña, String numero_contacto, Path foto) {
         this.id = id;
         this.nombre = nombre;
         this.contraseña = contraseña;
         this.numero_contacto = numero_contacto;
         this.foto = foto;
         setAcceso(false); //False indica que es un usuario.
     }
     public boolean registrarse(String nombre, String contra, String numero, DB db){
        if(db.getUsername(nombre) == null){
            setNombre(nombre);
            setContraseña(contra);
            setNumero_contacto(numero);
=======
     *
       */
     public Usuario(int id, String nombre, String contraseña, String numero_contacto, Path foto) {
        this.id = id;
        this.nombre = nombre;
        this.contraseña = contraseña;
        this.numero_contacto = numero_contacto;
        this.foto = foto;
        setAcceso(false); //False indica que es un usuario. 
     }

     public boolean registrarse(String nombre, String contraseña, String numero_contacto, Path foto) {
        Usuario user = DB.getUsuario(nombre);
        if(user == null){
            this.nombre = nombre;
            this.contraseña = contraseña;
            this.numero_contacto = numero_contacto;
            this.foto = foto;
>>>>>>> Stashed changes
            setAcceso(false);
            return true;
        }
        System.out.println("No se pudo registrar el usuario.");
        return false;
     }
<<<<<<< Updated upstream
     public boolean login(String nombre, String contra){
        if(this.nombre.equals(nombre) && contraseña.equals(contra)){
            return true;
=======
     
     public boolean login(String nombre, String contra) {
        Usuario user = DB.getUsuario(nombre);
        if(user != null){
            return this.nombre.equals(nombre) && contraseña.equals(contra);    
>>>>>>> Stashed changes
        }
        System.out.println("Usuario y/o contraseña incorrecto.");
        return false;
     }

     /**
      * Getters y Setters
      */
 
     public int getId() {
         return id;
     }
 
     public void setId(int id) {
         this.id = id;
     }
     public String getNombre() {
         return nombre;
     }
     public void setNombre(String nombre) {
         this.nombre = nombre;
     }
     public String getContraseña() {
         return contraseña;
     }
     public void setContraseña(String contraseña) {
         this.contraseña = contraseña;
     }
     public String getNumero_contacto() {
         return numero_contacto;
     }
     public void setNumero_contacto(String numero_contacto) {
         this.numero_contacto = numero_contacto;
     }
     public boolean isAcceso() {
         return acceso;
     }
     public void setAcceso(boolean acceso) {
         this.acceso = acceso;
     }
    public Path getFoto() {
        return foto;
    }
    public void setFoto(Path foto) {
        this.foto = foto;
    }
     
}