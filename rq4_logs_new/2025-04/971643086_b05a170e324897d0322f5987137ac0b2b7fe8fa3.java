import ar.edu.um.biblioteca.Enums.Estado;
import ar.edu.um.biblioteca.Models.*;
import org.junit.jupiter.api.*;
import org.mockito.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class PrestamoTest {
    private Libro libro;

    @Mock
    private Catalogo catalogo; // Crea un mock de la clase Catalogo

    @InjectMocks
    private SistemaPrestamos sistemaPrestamos; // Inyecta el mock de Catalogo en el sistema de préstamos

    @BeforeEach
    void setUp() {
        // Inicializa los mocks antes de cada test
        MockitoAnnotations.openMocks(this);
        //MockitoAnnotations.initMocks(this);
        libro = new Libro("978-3-16-148410-0", "Clean Code", "Robert C. Martin");
    }

    @Test
    void testPrestarLibro() {
        // Cada vez que alguien llame a catalogo.buscarPorIsbn("978..."), devuelve el libro específico.
        // Simula que el libro existe en el catalogo y se encuentra disponible
        when(catalogo.buscarPorIsbn("978-3-16-148410-0")).thenReturn(libro);

        // Intenta prestar el libro
        Prestamo prestamo = sistemaPrestamos.prestarLibro("978-3-16-148410-0");

        // Verifica que se haya creado un prestamo (no sea null)
        assertNotNull(prestamo);
        // Verifica que se haya llamado al metodo buscarPorIsbn
        verify(catalogo).buscarPorIsbn("978-3-16-148410-0");
        // Verifica que el estado del libro haya cambiado a PRESTADO
        assertEquals(Estado.PRESTADO, libro.getEstado());
    }

    @Test
    void testDevolverLibro() {
        // Configura el libro como prestado previamente
        libro.setEstado(Estado.PRESTADO);

        // Simula que el libro existe en el catalogo y se puede devolver
        when(catalogo.buscarPorIsbn("978-3-16-148410-0")).thenReturn(libro);

        // Intenta devolver el libro
        sistemaPrestamos.devolverLibro("978-3-16-148410-0");

        // Verificamos que el estado del libro haya cambiado a DISPONIBLE
        assertEquals(Estado.DISPONIBLE, libro.getEstado());
        // Verificamos que se haya llamado al metodo buscarPorIsbn en el catalogo
        verify(catalogo).buscarPorIsbn("978-3-16-148410-0");
    }

    @Test
    void testDevolverLibroNoPrestado() {
        // El libro esta disponible (no fue prestado)
        libro.setEstado(Estado.DISPONIBLE);

        // Simula que el libro existe pero esta disponible, por lo que no se puede devolver
        when(catalogo.buscarPorIsbn("978-3-16-148410-0")).thenReturn(libro);

        // Verificamos que se lance una IllegalStateException porque el libro no esta prestado
        assertThrows(IllegalStateException.class, () -> sistemaPrestamos.devolverLibro("978-3-16-148410-0"));
    }

    @Test
    void testDevolverLibroNoExistente() {
        // Simula que no se encuentra ningun libro con el ISBN dado
        when(catalogo.buscarPorIsbn("978-3-16-148410-0")).thenReturn(null);

        // Verificamos que se lance una IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> sistemaPrestamos.devolverLibro("978-3-16-148410-0"));
    }

    @Test
    void testDevolverLibroYaDevuelto() {
        // El libro esta disponible, es decir, ya fue devuelto
        libro.setEstado(Estado.DISPONIBLE);

        // Simula que el catalogo retorna un libro ya devuelto
        when(catalogo.buscarPorIsbn("978-3-16-148410-0")).thenReturn(libro);

        // Verificamos que se lance una IllegalStateException porque el libro ya esta disponible
        assertThrows(IllegalStateException.class, () -> sistemaPrestamos.devolverLibro("978-3-16-148410-0"));
    }

    @Test
    void testPrestarLibroYaPrestado() {
        // El libro ya esta prestado
        libro.setEstado(Estado.PRESTADO);

        // Simula que el catalogo devuelve un libro ya prestado
        when(catalogo.buscarPorIsbn("978-3-16-148410-0")).thenReturn(libro);

        // Verificamos que se lance una IllegalStateException porque el libro ya esta prestado
        assertThrows(IllegalStateException.class, () -> sistemaPrestamos.prestarLibro("978-3-16-148410-0"));
    }

    @Test
    void testPrestarLibroNoExistente() {
        // Simula que el libro no se encuentra en el catalogo
        when(catalogo.buscarPorIsbn("978-3-16-148410-0")).thenReturn(null);

        // Verificamos que se lance una IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> sistemaPrestamos.prestarLibro("978-3-16-148410-0"));
    }
}