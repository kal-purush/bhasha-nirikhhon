package ar.edu.um.biblioteca;

public class Libro {
    private String ISBN;
    private String titulo;
    private String autor;
    private EstadoLibro estado;

    // Constructor
    public Libro(String ISBN, String titulo, String autor, EstadoLibro estado) {
        this.ISBN = ISBN;
        this.titulo = titulo;
        this.autor = autor;
        this.estado = estado;
    }

    // Getters y Setters
    public String getISBN() {
        return ISBN;
    }
    public void setISBN(String ISBN) {
        this.ISBN = ISBN;
    }
    public String getTitulo() {
        return titulo;
    }
    public void setTitulo(String titulo) {
        this.titulo = titulo;
    }
    public String getAutor() {
        return autor;
    }
    public void setAutor(String autor) {
        this.autor = autor;
    }
    public EstadoLibro getEstado() {
        return estado;
    }
    public void setEstado(EstadoLibro estado) {
        this.estado = estado;
    }

    public enum EstadoLibro {
        DISPONIBLE,
        PRESTADO
    }
}