package um.programacion2.prestamo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;

import um.programacion2.TestConfig;
import um.programacion2.util.TestDataFactory;
import um.programacion2.exception.PrestamoNoEncontrado;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestConfig.class)
public class PrestamoControllerIntegrationTest {
    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PrestamoService prestamoService;

    @Autowired
    private PrestamoRepository prestamoRepository;

    static Stream<Prestamo> providerPrestamos() {
        return TestDataFactory.createPrestamos().stream();
    }

    @BeforeEach
    public void setup() {
        // Limpiar datos existentes
        ((PrestamoRepositoryImpl)prestamoRepository).limpiarTodo();

        // Cargar datos de prueba
        List<Prestamo> prestamos = TestDataFactory.createPrestamos();
        for (Prestamo prestamo : prestamos) {
            prestamoService.guardar(prestamo);
        }
    }

    @Test
    public void contextLoads() {
        // Solo verifica que el contexto cargue correctamente
    }

    @ParameterizedTest
    @MethodSource("providerPrestamos")
    public void testCrearPrestamo(Prestamo prestamo) {
        // Act
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "/api/prestamos", prestamo, Void.class);

        // Assert
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertTrue(response.getHeaders().getLocation().toString().contains("/api/prestamos/"));

        // Verificación adicional con el servicio real
        Long id = prestamo.getId();
        Prestamo guardado = prestamoService.buscarPorId(id);
        assertNotNull(guardado);
        assertEquals(id, guardado.getId());
    }

    @ParameterizedTest
    @MethodSource("providerPrestamos")
    public void testBuscarPorId_ConPrestamoExistente(Prestamo prestamo) {
        // Primero guardamos el préstamo para asegurarnos de que exista
        prestamoService.guardar(prestamo);
        
        // Act
        ResponseEntity<Prestamo> response = restTemplate.getForEntity(
                "/api/prestamos/" + prestamo.getId(), Prestamo.class);
        
        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(prestamo.getId(), response.getBody().getId());
        assertEquals(prestamo.getLibro().getId(), response.getBody().getLibro().getId());
        assertEquals(prestamo.getUsuario().getId(), response.getBody().getUsuario().getId());
    }

    @ParameterizedTest
    @MethodSource("providerPrestamos")
    public void testBuscarPorId_ConPrestamoInexistente(Prestamo prestamo) {
        // Act
        long idInexistente = 99999L;
        ResponseEntity<String> response = restTemplate.getForEntity(
                "/api/prestamos/" + idInexistente, String.class);
        
        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertTrue(response.getBody().contains("Prestamo no encontrado con ID: " + idInexistente));
    }

    @ParameterizedTest
    @MethodSource("providerPrestamos")
    public void testEliminarPrestamo_ConPrestamoExistente(Prestamo prestamo) {
        // Primero guardamos el préstamo para asegurarnos de que exista
        prestamoService.guardar(prestamo);
        
        // Act
        ResponseEntity<Void> response = restTemplate.exchange(
                "/api/prestamos/" + prestamo.getId(), 
                HttpMethod.DELETE,
                null,
                Void.class);
        
        // Assert
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        
        // Verificar que el préstamo ha sido eliminado
        try {
            prestamoService.buscarPorId(prestamo.getId());
            fail("El préstamo debería haber sido eliminado");
        } catch (PrestamoNoEncontrado e) {
            // Excepción esperada
        }
    }

    @ParameterizedTest
    @MethodSource("providerPrestamos")
    public void testEliminarPrestamo_ConPrestamoInexistente(Prestamo prestamo) {
        // Act
        long idInexistente = 99999L;
        ResponseEntity<String> response = restTemplate.exchange(
                "/api/prestamos/" + idInexistente,
                HttpMethod.DELETE,
                null,
                String.class);
        
        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertTrue(response.getBody().contains("Prestamo no encontrado con ID: " + idInexistente));
    }

    @Test
    public void testListarTodosLosPrestamos() {
        // Preparar datos - crear algunos préstamos
        List<Prestamo> prestamos = TestDataFactory.createPrestamos();
        for (Prestamo prestamo : prestamos) {
            prestamoService.guardar(prestamo);
        }
        
        // Act
        ResponseEntity<Prestamo[]> response = restTemplate.getForEntity(
                "/api/prestamos", Prestamo[].class);
        
        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().length >= prestamos.size(), 
                   "Deberían haber al menos " + prestamos.size() + " préstamos");
    }

    @ParameterizedTest
    @MethodSource("providerPrestamos")
    public void testActualizarPrestamo_ConPrestamoExistente(Prestamo prestamo) {
        // Primero guardamos el préstamo para asegurarnos de que exista
        prestamoService.guardar(prestamo);
        
        // Modificar préstamo (por ejemplo, cambiar la fecha de devolución)
        prestamo.setFechaPrestamo("2023-10-01");
        prestamo.setFechaDevolucion("2023-10-08");
        
        // Act
        ResponseEntity<Void> response = restTemplate.exchange(
                "/api/prestamos/" + prestamo.getId(),
                HttpMethod.PUT,
                new HttpEntity<>(prestamo),
                Void.class);
        
        // Assert
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        
        // Verificar que se actualizó
        Prestamo actualizado = prestamoService.buscarPorId(prestamo.getId());
        assertEquals("2023-10-01", actualizado.getFechaPrestamo());
        assertEquals("2023-10-08", actualizado.getFechaDevolucion());
    }

    @ParameterizedTest
    @MethodSource("providerPrestamos")
    public void testActualizarPrestamo_ConPrestamoInexistente(Prestamo prestamo) {
        // Act
        long idInexistente = 99999L;
        ResponseEntity<String> response = restTemplate.exchange(
                "/api/prestamos/" + idInexistente,
                HttpMethod.PUT,
                new HttpEntity<>(prestamo),
                String.class);
        
        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertTrue(response.getBody().contains("Prestamo no encontrado con ID: " + idInexistente));
    }
}