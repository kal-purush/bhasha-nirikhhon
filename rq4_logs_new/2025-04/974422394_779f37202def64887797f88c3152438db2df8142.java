import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CalculadoraTest {
    
    @BeforeEach
    void setUp() {
    }

    @Test
    void testSumar() {
        assertEquals(Calculadora.sumar(3, 4), 7);
    }

    @Test
    void testDividirPorCero() {
        assertThrows(ArithmeticException.class, () -> {Calculadora.dividir(3, 0);});
    }

    @Test
    void testResolverCuadraticaNum3Cero() {
        assertThrows(ArithmeticException.class, () -> {Calculadora.resolverCuadratica(, 0);});
    }

    @Test
    void testResolverCuadratica() {
        assertEquals();
    }
}