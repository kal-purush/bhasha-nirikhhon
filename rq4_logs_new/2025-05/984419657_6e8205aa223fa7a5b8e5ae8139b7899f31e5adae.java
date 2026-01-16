package clasesProyecto;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.time.Year;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Clase de pruebas unitarias para la clase Simulador en el proyecto LaLiga Fantasy.
 * Contiene tests para verificar la carga de equipos, manejo de temporadas y cálculos relacionados.
 */
class pagina01PrincipalTest {

    private Simulador simulador;

    /**
     * Metodo que se ejecuta antes de cada prueba para configurar el entorno.
     * Inicializa la conexión a la base de datos y el objeto Simulador.
     *
     * @throws SQLException si hay error al conectar con la base de datos.
     */
    @BeforeEach
    public void setUp() throws SQLException {
        ConexionMySQL conexion = new ConexionMySQL("root", "1234", "laliga_fantasy");
        conexion.conectar();

        simulador = new Simulador();
        simulador.conexion = conexion;
    }

    /**
     * Prueba que verifica que los equipos se cargan correctamente desde la base de datos.
     * Se asegura que la lista no sea nula ni esté vacía, y que los datos de los equipos sean válidos.
     */
    @Test
    public void test_CargarEquiposDesdeBD() {
        simulador.cargarEquiposDesdeBD();
        List<Equipo> equipos = simulador.equiposLaLiga;

        assertNotNull(equipos);
        assertFalse(equipos.isEmpty(), "Debe haber al menos un equipo cargado desde la base de datos");

        // Verificar algún dato específico si lo conoces
        Equipo primero = equipos.getFirst();
        assertNotNull(primero.getNombre());
        assertTrue(primero.getValoracion() >= 0 && primero.getValoracion() <= 5);
    }

    /**
     * Prueba para verificar si una temporada existe en la base de datos.
     * Comprueba tanto el caso de una temporada que sí existe como una que no.
     */
    @Test
    public void test_ExisteTemporada(){
        // Temporada que sí existe en tu BD, por ejemplo "25/26"
        String temporadaExistente = "25/26";  // Cambia por alguna temporada real que tengas en la BD
        assertTrue(simulador.existeTemporada(temporadaExistente), "Debe existir la temporada " + temporadaExistente);

        // Temporada que no existe
        String temporadaNoExistente = "99/99";
        assertFalse(simulador.existeTemporada(temporadaNoExistente), "No debe existir la temporada " + temporadaNoExistente);
    }

    /**
     * Prueba para verificar el comportamiento del método obtenerOTomarTemporada cuando no hay temporadas previas.
     * Se asegura que se cree una nueva temporada y que esta exista en la base de datos.
     *
     * @throws SQLException si hay error en la manipulación de la base de datos.
     */
    @Test
    public void test_ObtenerOTomarTemporada_CasoSinTemporadasPrevias() throws SQLException {
        // Asegurarse que no haya temporadas en la BD
        simulador.conexion.ejecutarInsertDeleteUpdate("DELETE FROM temporadas");

        // Llamar método, debería crear la primera temporada según tu cálculo
        String temporadaNueva = simulador.obtenerOTomarTemporada();

        // Verificar que la temporada no es nula
        assertNotNull(temporadaNueva);

        // Verificar que la temporada ahora existe en la BD
        assertTrue(simulador.existeTemporada(temporadaNueva));
    }

    /**
     * Prueba para verificar el método obtenerOTomarTemporada cuando ya existe una temporada en la base de datos.
     * Se asegura que se cree la siguiente temporada y que esta también exista en la base de datos.
     *
     * @throws SQLException si hay error en la manipulación de la base de datos.
     */
    @Test
    public void test_ObtenerOTomarTemporada_CasoConTemporadaExistente() throws SQLException {
        // Limpiar y crear una temporada manualmente
        simulador.conexion.ejecutarInsertDeleteUpdate("DELETE FROM temporadas");
        String temporadaExistente = "23/24";
        simulador.conexion.ejecutarInsertDeleteUpdate(
                String.format("INSERT INTO temporadas (id_temporada) VALUES ('%s')", temporadaExistente));

        // Llamar metodo, debería devolver la siguiente temporada
        String temporadaNueva = simulador.obtenerOTomarTemporada();

        // Debe ser diferente a la que existía
        assertNotEquals(temporadaExistente, temporadaNueva);

        // La nueva temporada también debe existir en la BD
        assertTrue(simulador.existeTemporada(temporadaNueva));
    }

    /**
     * Prueba que verifica el formato y valor devuelto por obtenerTemporadaNombre según el año actual.
     */
    @Test
    public void test_ObtenerTemporadaNombre() {
        Simulador simulador = new Simulador();

        // Obtener valores esperados según el año actual
        int anioActual = Year.now().getValue() % 100;
        int siguienteAnio = (anioActual + 1) % 100;
        String esperado = String.format("%02d/%02d", anioActual, siguienteAnio);

        // Llamar al metodo
        String resultado = simulador.obtenerTemporadaNombre();

        // Verificar que el resultado es igual al esperado
        assertEquals(esperado, resultado);
    }

    /**
     * Prueba que verifica el cálculo de la primera temporada desde el año actual.
     */
    @Test
    public void test_CalcularPrimeraTemporadaDesdeAnioActual() {
        Simulador simulador = new Simulador();

        int anioActual = Year.now().getValue() % 100;
        int siguienteAnio = (anioActual + 1) % 100;
        String esperado = String.format("%02d/%02d", anioActual, siguienteAnio);

        String resultado = simulador.calcularPrimeraTemporadaDesdeAnioActual();

        assertEquals(esperado, resultado, "La temporada calculada debe coincidir con el formato esperado");
    }

    /**
     * Prueba que verifica el cálculo de la siguiente temporada dado un nombre de temporada actual.
     * Se prueban varios casos para asegurar el correcto funcionamiento, incluyendo casos límite.
     */
    @Test
    public void test_CalcularSiguienteTemporada() {
        Simulador simulador = new Simulador();

        // Caso normal
        String temporadaActual = "23/24";
        String esperado = "24/25";
        String resultado = simulador.calcularSiguienteTemporada(temporadaActual);
        assertEquals(esperado, resultado);

        // Caso con valores cercanos a 99 para probar el módulo
        temporadaActual = "99/00";
        esperado = "00/01";
        resultado = simulador.calcularSiguienteTemporada(temporadaActual);
        assertEquals(esperado, resultado);

        // Caso con valores 09/10 para verificar el formato con cero a la izquierda
        temporadaActual = "09/10";
        esperado = "10/11";
        resultado = simulador.calcularSiguienteTemporada(temporadaActual);
        assertEquals(esperado, resultado);
    }
}