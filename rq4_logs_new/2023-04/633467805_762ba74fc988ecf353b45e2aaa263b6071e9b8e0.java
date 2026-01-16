package a_vegetable_garden.modele.legumes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
class FamillesLegumeTest {
    FamillesLegume legumeFeuilles = FamillesLegume.LEGUME_FEUILLES;
    FamillesLegume legumeRacines = FamillesLegume.LEGUME_RACINES;
    FamillesLegume legumeAFruits = FamillesLegume.LEGUME_A_FRUITS;
    FamillesLegume legumeGrimpant = FamillesLegume.LEGUME_GRIMPANT;
    FamillesLegume legumeABulbes = FamillesLegume.LEGUME_A_BULBES;
    FamillesLegume legumeATiges = FamillesLegume.LEGUME_A_TIGES;

    @BeforeEach
    void setUp() {
        legumeFeuilles = FamillesLegume.LEGUME_FEUILLES;
        legumeRacines = FamillesLegume.LEGUME_RACINES;
        legumeAFruits = FamillesLegume.LEGUME_A_FRUITS;
        legumeGrimpant = FamillesLegume.LEGUME_GRIMPANT;
        legumeABulbes = FamillesLegume.LEGUME_A_BULBES;
        legumeATiges = FamillesLegume.LEGUME_A_TIGES;
    }

    @Test
    @DisplayName("Test de la famille de légume feuilles")
    void testLegumeFeuilles() {
        assertEquals(0.5f, legumeFeuilles.getHumiditeMin());
        assertEquals(1f, legumeFeuilles.getHumiditeMax());
        assertEquals(0f, legumeFeuilles.getEnsoleillementMin());
        assertEquals(0.5f, legumeFeuilles.getEnsoleillementMax());
        assertEquals(10f, legumeFeuilles.getTemperatureMin());
        assertEquals(25f, legumeFeuilles.getTemperatureMax());
    }

    @Test
    @DisplayName("Test de la famille de légume racines")
    void testLegumeRacines() {
        assertEquals(0f, legumeRacines.getHumiditeMin());
        assertEquals(0.5f, legumeRacines.getHumiditeMax());
        assertEquals(0f, legumeRacines.getEnsoleillementMin());
        assertEquals(0.5f, legumeRacines.getEnsoleillementMax());
        assertEquals(5f, legumeRacines.getTemperatureMin());
        assertEquals(25f, legumeRacines.getTemperatureMax());
    }

    @Test
    @DisplayName("Test de la famille de légume à fruits")
    void testLegumeAFruits() {
        assertEquals(0.5f, legumeAFruits.getHumiditeMin());
        assertEquals(1f, legumeAFruits.getHumiditeMax());
        assertEquals(0.5f, legumeAFruits.getEnsoleillementMin());
        assertEquals(1f, legumeAFruits.getEnsoleillementMax());
        assertEquals(15f, legumeAFruits.getTemperatureMin());
        assertEquals(35f, legumeAFruits.getTemperatureMax());
    }

    @Test
    @DisplayName("Test de la famille de légume grimpant")
    void testLegumeGrimpant() {
        assertEquals(0.5f, legumeGrimpant.getHumiditeMin());
        assertEquals(1f, legumeGrimpant.getHumiditeMax());
        assertEquals(0.5f, legumeGrimpant.getEnsoleillementMin());
        assertEquals(1f, legumeGrimpant.getEnsoleillementMax());
        assertEquals(15f, legumeGrimpant.getTemperatureMin());
        assertEquals(35f, legumeGrimpant.getTemperatureMax());
    }

    @Test
    @DisplayName("Test de la famille de légume à bulbes")
    void testLegumeABulbes() {
        assertEquals(0f, legumeABulbes.getHumiditeMin());
        assertEquals(0.5f, legumeABulbes.getHumiditeMax());
        assertEquals(0f, legumeABulbes.getEnsoleillementMin());
        assertEquals(0.5f, legumeABulbes.getEnsoleillementMax());
        assertEquals(10f, legumeABulbes.getTemperatureMin());
        assertEquals(25f, legumeABulbes.getTemperatureMax());
    }

    @Test
    @DisplayName("Test de la famille de légume à tiges")
    void testLegumeATiges() {
        assertEquals(0f, legumeATiges.getHumiditeMin());
        assertEquals(0.5f, legumeATiges.getHumiditeMax());
        assertEquals(0.5f, legumeATiges.getEnsoleillementMin());
        assertEquals(1f, legumeATiges.getEnsoleillementMax());
        assertEquals(10f, legumeATiges.getTemperatureMin());
        assertEquals(30f, legumeATiges.getTemperatureMax());
    }

}