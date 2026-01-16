package com.Character;

import com.Exception.InvalidNiveauException;
import com.Exception.TooHighAttributesException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CharacterTest {

    private class CharacterMock extends Character {

        public CharacterMock(int niveau, int force, int agilite, int intelligence, String nom)
                throws TooHighAttributesException, InvalidNiveauException {
            super(niveau, force, agilite, intelligence, nom);
        }

        @Override
        public void attaqueBasique(Character cible) {

        }

        @Override
        public void attaqueSpeciale(Character cible) {

        }
    }

    private Character herculeMock;

    @BeforeEach
    void init() {
        try {
            herculeMock = new CharacterMock(10, 5, 3, 2, "Hercule");
        } catch (TooHighAttributesException | InvalidNiveauException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    //teste la création d'un personnage et vérifie qu'il est en vie avec le bon nombre de points de vie
    @Test
    @DisplayName("Création de personnage fonctionnel")
    @Tag("constructor-testing")
    void testCharacterOk() {
        assertTrue(herculeMock.estVivant());
        assertEquals(herculeMock.getVie(), 50);
        assertEquals(herculeMock.getNom(), "Hercule");
    }

    //teste la création d'un personnage avec un niveau trop élevé
    @Test
    @DisplayName("Création de personnage avec un niveau trop haut")
    @Tag("constructor-testing")
    void testCharacterNokLVLTropHaut() {
        Throwable throwable = assertThrows(InvalidNiveauException.class, () -> new CharacterMock(2000, 10, 10, 10, "error"));
        assertAll(
                () -> assertEquals("Le niveau 2000 n'est pas autorisé (max 100)", throwable.getMessage())
        );

    }

    //teste la création d'un héro avec un niveau trop élevé
    @Test
    @DisplayName("Création de personnage avec des attributs trop haut")
    @Tag("constructor-testing")
    void testHeroNokTropAttributs() {
        Throwable throwable = assertThrows(TooHighAttributesException.class, () -> new CharacterMock(2, 10, 10, 10, "error"));
        assertAll(
                () -> assertEquals("La somme des attribues (30) ne peux pas être supérieure au niveau du héro (2)", throwable.getMessage())
        );

    }

    @Test
    void testPrendreDegats() {
        assertTrue(herculeMock.estVivant());
        assertEquals(herculeMock.getVie(), 50);

        herculeMock.prendreDegats(20);
        assertTrue(herculeMock.estVivant());
        assertEquals(herculeMock.getVie(), 30);
    }

    @Test
    void testEstVivantAvecHPSupA0() {
        assertTrue(herculeMock.estVivant());

        herculeMock.vie = 10;
        assertTrue(herculeMock.estVivant());

        herculeMock.vie = 3000;
        assertTrue(herculeMock.estVivant());
    }

    @Test
    void testEstVivantSansHP() {
        herculeMock.vie = 0;
        assertFalse(herculeMock.estVivant());
    }

    @Test
    void testPrendreDegatsMortels() {
        assertTrue(herculeMock.estVivant());
        assertEquals(herculeMock.getVie(), 50);

        herculeMock.prendreDegats(20);
        assertTrue(herculeMock.estVivant());
        assertEquals(herculeMock.getVie(), 30);

        herculeMock.prendreDegats(20);
        assertTrue(herculeMock.estVivant());
        assertEquals(herculeMock.getVie(), 10);

        herculeMock.prendreDegats(20);
        assertFalse(herculeMock.estVivant());
        assertEquals(herculeMock.getVie(), 0);
    }
}