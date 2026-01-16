import ar.edu.um.biblioteca.Enums.Estado;
import ar.edu.um.biblioteca.Models.Catalogo;
import ar.edu.um.biblioteca.Models.Libro;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CatalogoTest {
    Catalogo catalogo;
    Libro libro1;
    Libro libro2;
    Libro libro3;
    Libro libro4;

    @BeforeEach
    void setUp() {
        catalogo = new Catalogo();
        libro1 = new Libro("978-3-16-148410-0", "Clean Code", "Robert C. Martin");
        libro2 = new Libro("978-0-13-235088-4", "Clean Architecture", "Robert C. Martin");
        libro3 = new Libro("978-0-03-734227-5", "Percy Jackson y el ladrón del rayo", "Rick Riordan");
        libro4 = new Libro("978-0-02-785526-8", "Percy Jackson y el mar de los monstruos", "Rick Riordan");
        catalogo.agregarLibro(libro1);
        catalogo.agregarLibro(libro2);
        catalogo.agregarLibro(libro3);
        catalogo.agregarLibro(libro4);
        // Cambio el estado del libro a prestado
        libro3.setEstado(Estado.PRESTADO);
        libro4.setEstado(Estado.PRESTADO);
    }

    @Test
    void testBuscarPorIsbnExitoso() {
        Libro libroE = catalogo.buscarPorIsbn("978-3-16-148410-0");
        // Verifica que el libro no sea null
        assertNotNull(libroE);
        // Verifica que el título sea el esperado
        assertEquals("Clean Code", libroE.getTitulo());
    }

    @Test
    void testBuscarPorIsbnFallido() {
        // Busca un libro que no existe en el catalogo
        Libro libroF = catalogo.buscarPorIsbn("978-1-23-456789-0");
        assertNull(libroF);
    }

    @Test
    void testObtenerLibrosDisponibles() {
        // Obtener los libros disponibles
        List<Libro> disponibles = catalogo.obtenerLibrosDisponibles();

        // Verificar que el catálogo solo tenga libros disponibles
        assertEquals(2, disponibles.size());
        assertEquals("Clean Code", disponibles.get(0).getTitulo());
        assertEquals("Clean Architecture", disponibles.get(1).getTitulo());
    }

    @Test
    void testObtenerLibrosPrestados() {
        // Obtener los libros prestados
        List<Libro> prestados = catalogo.obtenerLibrosPrestados();

        // Verificar que el catálogo solo tenga libros prestados
        assertEquals(2, prestados.size());
        assertEquals("Percy Jackson y el ladrón del rayo", prestados.get(0).getTitulo());
        assertEquals("Percy Jackson y el mar de los monstruos", prestados.get(1).getTitulo());

    }

    @Test
    void testObtenerTodosLosLibros() {
        // Obtiene todos los libros del catalogo
        List<Libro> todosLosLibros = catalogo.getLibros();

        // Verifico que la lista tenga 3 libros
        assertEquals(4, todosLosLibros.size());
        assertEquals("Clean Code", todosLosLibros.get(0).getTitulo());
        assertEquals("Clean Architecture", todosLosLibros.get(1).getTitulo());
        assertEquals("Percy Jackson y el ladrón del rayo", todosLosLibros.get(2).getTitulo());
        assertEquals("Percy Jackson y el mar de los monstruos", todosLosLibros.get(3).getTitulo());
    }

    @Test
    void testAgregarLibro() {
        Libro libroNuevo = new Libro("978-0-00-000000-1", "El Principito", "Antoine de Saint-Exupéry");
        catalogo.agregarLibro(libroNuevo);

        // Verifica que el libro se haya agregado correctamente al catálogo
        assertTrue(catalogo.getLibros().contains(libroNuevo));
        assertEquals(5, catalogo.getLibros().size()); // El catalogo ahora debería tener 5 libros
    }

}