package server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import server.Tile.Bag;

public class BoardTest {

    final String DICTIONARY_1 = "test/resources/dictionary_1.txt";
    final String DICTIONARY_2 = "test/resources/dictionary_2.txt";

    public Tile[] stringToTiles(String string) {
        Bag bag = new Bag();
        return string.chars().mapToObj(c->(char)c).map(bag::getTile).toArray(Tile[]::new);
    }

    @Test
    public void testDictionaryLegalWordLegal() {
        Board board = new Board(new String[]{DICTIONARY_1});
        Tile[] tiles = stringToTiles("HELLO");
        Word word = new Word(tiles, 0, 0, false);
        Assertions.assertTrue(board.dictionaryLegal(word));
    }

    @Test
    public void testDictionaryLegalWordIllegal() {
        Board board = new Board(new String[]{DICTIONARY_1});
        Tile[] tiles = stringToTiles("ME");
        Word word = new Word(tiles, 0, 0, false);
        Assertions.assertFalse(board.dictionaryLegal(word));
    }

    @Test
    public void testDictionaryLegalTwoDictionaries() {
        Board board = new Board(new String[]{DICTIONARY_1, DICTIONARY_2});
        Tile[] tiles = stringToTiles("FIRST");
        Word word = new Word(tiles, 0, 0, false);
        Assertions.assertTrue(board.dictionaryLegal(word));
    }
}