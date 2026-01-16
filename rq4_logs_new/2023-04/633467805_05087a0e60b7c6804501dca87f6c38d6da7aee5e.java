package a_vegetable_garden.modele.player;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
class InventoryTest {
    Inventory inventory;

    @BeforeEach
    public void setUp() {
        inventory = Inventory.getInstance();
    }

    @Test
    @DisplayName("Test du singleton d'inventaire")
    void testSingleton() {
        Inventory inventory2 = Inventory.getInstance();
        assertSame(inventory, inventory2, "Les deux instances d'inventaire doivent être identiques.");
    }

    @Test
    @DisplayName("Test de l'ajout de pièces")
    void addNbCoins(){
        float nbCoins = inventory.getNbCoins();
        inventory.addCoins(10);
        assertEquals(nbCoins + 10, inventory.getNbCoins(), "Le nombre de pièces doit être correctement incrémenté.");
    }

    @Test
    @DisplayName("Test de la suppression de pièces")
    void removeNbCoins(){
        float nbCoins = inventory.getNbCoins();
        inventory.removeCoinsIfEnough(10);
        assertEquals(nbCoins - 10, inventory.getNbCoins(), "Le nombre de pièces doit être correctement décrémenté.");
    }

    @Test
    @DisplayName("Test de la suppression de pièces avec un nombre plus grand que le nombre de pièces")
    void removeNbCoinsTooMuch(){
        float nbCoins = inventory.getNbCoins();
        inventory.removeCoinsIfEnough(nbCoins + 10);
        assertEquals(nbCoins, inventory.getNbCoins(), "Le nombre de pièces ne doit pas être décrémenté.");
    }

}