package ar.edu.um.biblioteca.Models;
import ar.edu.um.biblioteca.Enums.Estado;
import lombok.*;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
public class Catalogo {
    private List<Libro> libros = new ArrayList<>();

    // Metodo para agregar libro al catalogo
    public void agregarLibro(Libro libro) {
        libros.add(libro);
    }

    // Metodo para buscar libro por isbn (usando stream)
    public Libro buscarPorIsbn(String isbn) {
        return libros.stream()
                .filter(libro -> libro.getIsbn().equals(isbn))
                .findFirst()
                .orElse(null);
    }

    // Metodo para obtener todos los libros disponibles (usando stream)
    public List<Libro> obtenerLibrosDisponibles() {
        return libros.stream()
                .filter(libro -> libro.getEstado() == Estado.DISPONIBLE)
                .collect(Collectors.toList());
    }

    // Metodo para obtener todos los libros prestados (usando for) (hecho para practicar)
    public List<Libro> obtenerLibrosPrestados() {
        List<Libro> prestados = new ArrayList<>();
        for (Libro libro : libros) {
            if (libro.getEstado() == Estado.PRESTADO) {
                prestados.add(libro);
            }
        }
        return prestados;
    }

}