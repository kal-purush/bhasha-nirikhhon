package testes;

import cartas.Main;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    @Test
    void converteCartaParaInt() {
        Main carta = new Main();

        assertEquals(1, carta.converteCartaParaInt("A"));
        assertEquals(1, carta.converteCartaParaInt("a"));
        assertEquals(2, carta.converteCartaParaInt("2"));
        assertEquals(3, carta.converteCartaParaInt("3"));
        assertEquals(4, carta.converteCartaParaInt("4"));
        assertEquals(5, carta.converteCartaParaInt("5"));
        assertEquals(6, carta.converteCartaParaInt("6"));
        assertEquals(7, carta.converteCartaParaInt("7"));
        assertEquals(8, carta.converteCartaParaInt("8"));
        assertEquals(9, carta.converteCartaParaInt("9"));
        assertEquals(10, carta.converteCartaParaInt("10"));
        assertEquals(11, carta.converteCartaParaInt("J"));
        assertEquals(11, carta.converteCartaParaInt("j"));
        assertEquals(12, carta.converteCartaParaInt("Q"));
        assertEquals(12, carta.converteCartaParaInt("q"));
        assertEquals(13, carta.converteCartaParaInt("K"));
        assertEquals(13, carta.converteCartaParaInt("k"));
        assertEquals(13, carta.converteCartaParaInt("k"));
        assertEquals(-1, carta.converteCartaParaInt("kk"));
    }
}