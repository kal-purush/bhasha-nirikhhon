package com.example.list_it;

public class Listas {
    private String Titulo;
    private String Contenido;
    private long Fecha;

    public Listas(String Titulo, String Contenido, long Fecha){
        this.Titulo = Titulo;
        this.Contenido = Contenido;
        this.Fecha = Fecha;
    }

    public String getTitulo() {
        return Titulo;
    }

    public void setTitulo(String titulo) {
        Titulo = titulo;
    }

    public String getContenido() {
        return Contenido;
    }

    public void setContenido(String contenido) {
        Contenido = contenido;
    }

    public long getFecha() {
        return Fecha;
    }

    public void setFecha(long fecha) {
        Fecha = fecha;
    }
}