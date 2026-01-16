package doghost;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DogHostTest {

    private DogHost dogHost;

    @BeforeEach
    void setup() {
        dogHost = new DogHost("Evankleber", 2, 100);
    }

    @Test
    void adicionaDogMaxRacao() {
        assertTrue(dogHost.adicionaDog(new Dog("Kleytinhuhh", "Jorgeilson", 98)));
        assertFalse(dogHost.adicionaDog(new Dog("N-Man", "Lovecraft", 3)));
        assertTrue(dogHost.adicionaDog(new Dog("Hachiko", "Professor do Filme", 1)));
        assertFalse(dogHost.adicionaDog(new Dog("n sei", "faltou criatividade", 1)));
    }

    @Test
    void testConsultaValorHospedagem() {
        Dog d = new Dog("Kelvin", "Fahrenheit", 55);
        dogHost.adicionaDog(d);

        assertEquals(dogHost.consultaValorHospedagem(d, 2), 44);

        try {
            dogHost.consultaValorHospedagem(new Dog("Celsius", "ASD", 55), 5);
            fail();
        } catch (IllegalArgumentException iae) {
        }
    }

}