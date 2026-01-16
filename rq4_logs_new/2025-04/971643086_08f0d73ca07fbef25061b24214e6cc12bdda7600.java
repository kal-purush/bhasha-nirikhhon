import ar.edu.um.biblioteca.Models.*;
import org.junit.jupiter.api.*;
import org.mockito.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class UsuarioTest {
    private Usuario usuario;
    private Libro libro;

    @Mock
    private Catalogo catalogo; // Dependencia simulada del catálogo

    @Mock
    private SistemaPrestamos sistemaPrestamos; // Dependencia simulada del sistema de préstamos

    @InjectMocks
    private GestionUsuarios gestionUsuarios; // Clase que se testea, con dependencias inyectadas

    @BeforeEach
    void setUp() {
        // Inicializa las anotaciones de Mockito antes de cada test
        MockitoAnnotations.openMocks(this);
        usuario = new Usuario("Martina", "Rizzotti");
        gestionUsuarios.agregarUsuario(usuario);
        libro = new Libro("978-0-03-734227-5", "Percy Jackson y el ladrón del rayo", "Rick Riordan");
    }

    @Test
    void testRegistrarPrestamo() {
        // Configura el comportamiento simulado de los mocks
        when(catalogo.buscarPorIsbn("978-0-03-734227-5")).thenReturn(libro);
        when(sistemaPrestamos.prestarLibro("978-0-03-734227-5"))
                .thenReturn(new Prestamo(libro));

        // Llama al metodo que se quiere testear
        gestionUsuarios.registrarPrestamo("Martina", "Rizzotti" ,"978-0-03-734227-5");

        // Verifica que el metodo prestarLibro fue llamado con el ISBN correcto
        verify(sistemaPrestamos).prestarLibro("978-0-03-734227-5");
        // Verifica que el historial de préstamos del usuario se actualizó correctamente
        assertEquals(1, usuario.getHistorialPrestamos().size());
    }

    @Test
    void testRegistrarPrestamoUsuarioNoExiste() {
        // Verifica que se lanza una excepción cuando el usuario no existe
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            gestionUsuarios.registrarPrestamo("Valentina", "Rosales", "978-0-03-734227-5");
        });

        // Verifica que el mensaje de error es el esperado
        assertEquals("Usuario no encontrado", exception.getMessage());
    }

    @Test
    void testRegistrarPrestamoLibroNoDisponible() {
        // Configura el mock para lanzar una excepción si el libro no está disponible
        when(sistemaPrestamos.prestarLibro("978-0-03-734227-5"))
                .thenThrow(new IllegalStateException("Libro no disponible"));

        // Verifica que se lanza la excepción cuando se intenta prestar un libro no disponible
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            gestionUsuarios.registrarPrestamo("Martina", "Rizzotti", "978-0-03-734227-5");
        });

        // Verifica que el mensaje de error es el esperado
        assertEquals("Libro no disponible", exception.getMessage());
    }

}