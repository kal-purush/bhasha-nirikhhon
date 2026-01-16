package um.programacion2.usuario;

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
import um.programacion2.exception.UsuarioNoEncontrado;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestConfig.class)
public class UsuarioControllerIntegrationTest {
    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private UsuarioService usuarioService;

    @Autowired
    private UsuarioRepository usuarioRepository;

    static Stream<Usuario> providerUsuarios() {
        return TestDataFactory.createUsuarios().stream();
    }

    @BeforeEach
    public void setup() {
        // Limpiar datos existentes
        ((UsuarioRepositoryImpl)usuarioRepository).limpiarTodo();

        // Cargar datos de prueba
        List<Usuario> usuarios = TestDataFactory.createUsuarios();
        for (Usuario usuario : usuarios) {
            usuarioService.guardar(usuario);
        }
    }

    @Test
    public void contextLoads() {
        // Solo verifica que el contexto cargue correctamente
    }

    @ParameterizedTest
    @MethodSource("providerUsuarios")
    public void testCrearUsuario(Usuario usuario) {
        // Act
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "/api/usuarios", usuario, Void.class);

        // Assert
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertTrue(response.getHeaders().getLocation().toString().contains("/api/usuarios/"));

        // Verificación adicional con el servicio real
        Long id = usuario.getId();
        Usuario guardado = usuarioService.buscarPorId(id);
        assertNotNull(guardado);
        assertEquals(id, guardado.getId());
    }

    @ParameterizedTest
    @MethodSource("providerUsuarios")
    public void testBuscarPorId_ConUsuarioExistente(Usuario usuario) {
        // Primero guardamos el usuario para asegurarnos de que exista
        usuarioService.guardar(usuario);
        
        // Act
        ResponseEntity<Usuario> response = restTemplate.getForEntity(
                "/api/usuarios/" + usuario.getId(), Usuario.class);
        
        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(usuario.getId(), response.getBody().getId());
        assertEquals(usuario.getNombre(), response.getBody().getNombre());
    }

    @ParameterizedTest
    @MethodSource("providerUsuarios")
    public void testBuscarPorId_ConUsuarioInexistente(Usuario usuario) {
        // Act
        long idInexistente = 99999L;
        ResponseEntity<String> response = restTemplate.getForEntity(
                "/api/usuarios/" + idInexistente, String.class);
        
        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertTrue(response.getBody().contains("Usuario no encontrado con ID: " + idInexistente));
    }

    @ParameterizedTest
    @MethodSource("providerUsuarios")
    public void testEliminarUsuario_ConUsuarioExistente(Usuario usuario) {
        // Primero guardamos el usuario para asegurarnos de que exista
        usuarioService.guardar(usuario);
        
        // Act
        ResponseEntity<Void> response = restTemplate.exchange(
                "/api/usuarios/" + usuario.getId(), 
                HttpMethod.DELETE,
                null,
                Void.class);
        
        // Assert
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        
        // Verificar que el usuario ha sido eliminado
        try {
            usuarioService.buscarPorId(usuario.getId());
            fail("El usuario debería haber sido eliminado");
        } catch (UsuarioNoEncontrado e) {
            // Excepción esperada
        }
    }

    @ParameterizedTest
    @MethodSource("providerUsuarios")
    public void testEliminarUsuario_ConUsuarioInexistente(Usuario usuario) {
        // Act
        long idInexistente = 99999L;
        ResponseEntity<String> response = restTemplate.exchange(
                "/api/usuarios/" + idInexistente,
                HttpMethod.DELETE,
                null,
                String.class);
        
        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertTrue(response.getBody().contains("Usuario no encontrado con ID: " + idInexistente));
    }

    @Test
    public void testListarTodosLosUsuarios() {
        // Preparar datos - crear algunos usuarios
        List<Usuario> usuarios = TestDataFactory.createUsuarios();
        for (Usuario usuario : usuarios) {
            usuarioService.guardar(usuario);
        }
        
        // Act
        ResponseEntity<Usuario[]> response = restTemplate.getForEntity(
                "/api/usuarios", Usuario[].class);
        
        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
    }

    @ParameterizedTest
    @MethodSource("providerUsuarios")
    public void testActualizarUsuario_ConUsuarioExistente(Usuario usuario) {
        // Primero guardamos el usuario para asegurarnos de que exista
        usuarioService.guardar(usuario);
        
        // Modificar usuario
        String nuevoNombre = "Nombre actualizado - " + System.currentTimeMillis();
        usuario.setNombre(nuevoNombre);
        
        // Act
        ResponseEntity<Void> response = restTemplate.exchange(
                "/api/usuarios/" + usuario.getId(),
                HttpMethod.PUT,
                new HttpEntity<>(usuario),
                Void.class);
        
        // Assert
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        
        // Verificar que se actualizó
        Usuario actualizado = usuarioService.buscarPorId(usuario.getId());
        assertEquals(nuevoNombre, actualizado.getNombre());
    }

    @ParameterizedTest
    @MethodSource("providerUsuarios")
    public void testActualizarUsuario_ConUsuarioInexistente(Usuario usuario) {
        // Act
        long idInexistente = 99999L;
        ResponseEntity<String> response = restTemplate.exchange(
                "/api/usuarios/" + idInexistente,
                HttpMethod.PUT,
                new HttpEntity<>(usuario),
                String.class);
        
        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertTrue(response.getBody().contains("Usuario no encontrado con ID: " + idInexistente));
    }
}