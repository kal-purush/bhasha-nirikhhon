import clasesProyecto.Equipo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Year;

import static org.junit.jupiter.api.Assertions.*;

public class testUnitariosProyecto {

    private Equipo equipo1;
    private Equipo equipo2;

    @BeforeEach
    public void setUp() {
        equipo1 = new Equipo("FC Barcelona", 5, 0, 0, 0, 0, 0, 0, 0, 0);
        equipo2 = new Equipo("Real Betis", 4.5f, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void testGetNombre() {
        assertEquals("FC Barcelona", equipo1.getNombre());
    }

    @Test
    public void testSumarPuntos() {
        equipo2.sumarPuntos(5);
        equipo2.sumarPuntos(3);
        assertEquals(8, equipo2.getPuntos());
    }


    // Clase ficticia con el metodo a probar (puedes adaptarlo al nombre real)
    static class Utilidades {
        public String calcularTemporadaDesdeAnioActual() {
            int anioActual = Year.now().getValue() % 100;
            int siguienteAnio = (anioActual + 1) % 100;
            return String.format("%02d/%02d", anioActual, siguienteAnio);
        }
    }

    @Test
    public void testCalcularTemporadaDesdeAnioActual() {
        Utilidades util = new Utilidades();

        int anioActual = Year.now().getValue() % 100;
        int siguienteAnio = (anioActual + 1) % 100;
        String esperado = String.format("%02d/%02d", anioActual, siguienteAnio);

        assertEquals(esperado, util.calcularTemporadaDesdeAnioActual());
    }
}