package ar.edu.um.biblioteca.Models;
import ar.edu.um.biblioteca.Enums.Estado;
import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class SistemaPrestamos {
    private List<Prestamo> prestamos = new ArrayList<>();
    private Catalogo catalogo;

    // Constructor
    public SistemaPrestamos(Catalogo catalogo) {
        this.catalogo = catalogo;
    }

    // Metodo para prestar libro
    public Prestamo prestarLibro(String isbn) {
        // Busca el libro por ISBN en el catálogo
        Libro libro = catalogo.buscarPorIsbn(isbn);

        if (libro == null) {
            throw new IllegalArgumentException("El libro con ISBN [" + isbn + "] no existe.");
        }

        if (libro.getEstado() == Estado.DISPONIBLE) {
            Prestamo prestamo = new Prestamo(libro);
            prestamos.add(prestamo);
            libro.setEstado(Estado.PRESTADO);
            return prestamo;
        } else {
            throw new IllegalStateException("El libro no esta disponible para prestamo.");
        }
    };

    // Metodo para devolver libro
    public void devolverLibro(String isbn) {
        // Busca el libro en el catálogo
        Libro libro = catalogo.buscarPorIsbn(isbn);

        if (libro == null) {
            throw new IllegalArgumentException("El libro con ISBN [" + isbn + "] no existe.");
        }

        if (libro.getEstado() == Estado.PRESTADO) {
            libro.setEstado(Estado.DISPONIBLE);
        } else {
            throw new IllegalStateException("El libro no está actualmente prestado.");
        }
    }
}