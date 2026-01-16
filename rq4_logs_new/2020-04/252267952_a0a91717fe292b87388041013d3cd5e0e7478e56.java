
package com.mycompany.mensajes_app;


public class Usuarios {
    //Atributos
    private int idUsuario;
    private String correo;
    private String clave;
    private String nombreCompleto;
    //Constructores
    
    //Constructor General
    public Usuarios() {
    }
    
    //Constructor para borrar Usuario
    public Usuarios(int idUsuario) {
        this.idUsuario = idUsuario;
    }
    
    //Constructor para iniciar sesión
    public Usuarios(String correo, String clave) {
        this.correo = correo;
        this.clave = clave;
    }
    
    //Constructor para traer datos de inicio de sesión
    public Usuarios(String correo, String clave, String nombreCompleto) {
        this.correo = correo;
        this.clave = clave;
        this.nombreCompleto = nombreCompleto;
    }
    
    //Constructor para crear y editar usuario
    public Usuarios(int idUsuario, String correo, String clave, String nombreCompleto) {    
        this.idUsuario = idUsuario;
        this.correo = correo;
        this.clave = clave;
        this.nombreCompleto = nombreCompleto;
    }
    
    //Getters y Setters
    public int getIdUsuario() {
        return idUsuario;
    }

    public void setIdUsuario(int idUsuario) {
        this.idUsuario = idUsuario;
    }

    public String getCorreo() {
        return correo;
    }

    public void setCorreo(String correo) {
        this.correo = correo;
    }

    public String getClave() {
        return clave;
    }

    public void setClave(String clave) {
        this.clave = clave;
    }

    public String getNombreCompleto() {
        return nombreCompleto;
    }

    public void setNombreCompleto(String nombreCompleto) {
        this.nombreCompleto = nombreCompleto;
    }
    
    
    
}