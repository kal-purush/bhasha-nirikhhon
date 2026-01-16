package a_vegetable_garden.modele.legumes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AilTest {
    Ail ail;

    @BeforeEach
    void setUp() {
        ail = new Ail();
    }

    @Test
    @DisplayName("ail.getVariete() should return Varietes.ail")
    void getVariete() {
        assertEquals(Varietes.AIL, ail.getVariete());
    }

    @Test
    @DisplayName("ail.croissance() should do something")
    void croissance() {
        // TODO
    }
}