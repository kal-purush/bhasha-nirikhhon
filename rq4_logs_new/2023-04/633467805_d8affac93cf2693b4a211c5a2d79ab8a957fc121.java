package modele.legumes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import vueControleur.icon.IconNames;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class VarietesTest {
    Varietes salade;
    Varietes carotte;

    @BeforeEach
    void setUp() {
        salade = new Salade().getVariete();
        carotte = new Carotte().getVariete();
    }

    @Test
    @DisplayName("Varietes.SALADE should be at the same position as IconNames.SALADE")
    void testSalade() {
        assertEquals(IconNames.SALADE.ordinal(), salade.ordinal());
    }

    @Test
    @DisplayName("Varietes.CAROTTE should be at the same position as IconNames.CAROTTE")
    void testCarotte() {
        assertEquals(IconNames.CAROTTE.ordinal(), carotte.ordinal());
    }
}