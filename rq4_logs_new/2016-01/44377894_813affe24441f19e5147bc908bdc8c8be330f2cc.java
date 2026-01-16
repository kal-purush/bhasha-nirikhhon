package jttt.console;

import jttt.core.boarddisplayer.BoardDisplayer;
import jttt.core.board.Board;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class ConsoleBoardDisplayerTest {

    private FakeConsoleController uiSpy;
    private ConsoleDisplayStylerSpy stylerSpy;
    private BoardDisplayer boardDisplayer;
    private OutputStream output;
    private InputStream inputStream;
    private Writer writer;

    @Before
    public void setUp() {
        output = new ByteArrayOutputStream();
        writer = new OutputStreamWriter(output);
        inputStream = new ByteArrayInputStream("1".getBytes());
        uiSpy = new FakeConsoleController(null, inputStream, writer);
        stylerSpy = new ConsoleDisplayStylerSpy();
        boardDisplayer = new ConsoleBoardDisplayer(uiSpy);
    }

    @Test
    public void uiDisplayerIsAKindOfDisplayer() {
        assertEquals(true, boardDisplayer instanceof BoardDisplayer);
    }

    @Test
    public void displayBoardToUI() {
        boardDisplayer.updateBoardDisplay(new Board(3));
        assertEquals(true, uiSpy.hasBoardBeenDisplayedToUI());
    }
}