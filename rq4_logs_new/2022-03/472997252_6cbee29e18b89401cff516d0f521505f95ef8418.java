package com.company;

public class Mensajes {
    int id_mensaje;
    String mensaje;
    String autor_mensaje;
    String Fecha;

    public Mensajes(){

    }

    public Mensajes(String mensaje, String autor_mensaje, String fecha) {
        this.mensaje = mensaje;
        this.autor_mensaje = autor_mensaje;
        Fecha = fecha;
    }

    public int getId_mensaje() {
        return id_mensaje;
    }

    public void setId_mensaje(int id_mensaje) {
        this.id_mensaje = id_mensaje;
    }

    public String getMensaje() {
        return mensaje;
    }

    public void setMensaje(String mensaje) {
        this.mensaje = mensaje;
    }

    public String getAutor_mensaje() {
        return autor_mensaje;
    }

    public void setAutor_mensaje(String autor_mensaje) {
        this.autor_mensaje = autor_mensaje;
    }

    public String getFecha() {
        return Fecha;
    }

    public void setFecha(String fecha) {
        Fecha = fecha;
    }
}