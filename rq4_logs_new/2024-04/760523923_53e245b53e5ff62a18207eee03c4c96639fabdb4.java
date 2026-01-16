package seedu.address.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import org.junit.jupiter.api.Test;

public class CommandHistoryTest {
    private CommandHistoryStub commandHistoryStub = new CommandHistoryStub();

    @Test
    public void constructor() {
        assertEquals(Collections.emptyList(), commandHistoryStub.getCommandHistory());
    }

    @Test
    public void nextCommand_givesEmptyWhenOutOfBounds() {
        // make empty
        commandHistoryStub = new CommandHistoryStub();

        // cursor is definitely longer than the commands already typed
        commandHistoryStub.setCursorPosition(2);

        // should be empty
        assertTrue(commandHistoryStub.getNextCommand().isEmpty());

        // try with negative integers
        commandHistoryStub.setCursorPosition(-1);
        assertTrue(commandHistoryStub.getNextCommand().isEmpty());

        // try with non-empty history
        commandHistoryStub.add("find n/John Doe");
        commandHistoryStub.setCursorPosition(2);
        assertTrue(commandHistoryStub.getNextCommand().isEmpty());
        commandHistoryStub.setCursorPosition(-1);
        assertTrue(commandHistoryStub.getNextCommand().isEmpty());
    }

    @Test
    public void prevCommand_givesEmptyWhenOutOfBounds() {
        // try empty command history
        commandHistoryStub = new CommandHistoryStub();
        commandHistoryStub.setCursorPosition(2);
        assertTrue(commandHistoryStub.getPreviousCommand().isEmpty());
        commandHistoryStub.setCursorPosition(-1);
        assertTrue(commandHistoryStub.getPreviousCommand().isEmpty());

        // try non-empty historu
        commandHistoryStub.add("add n/John");
        commandHistoryStub.setCursorPosition(2);
        assertTrue(commandHistoryStub.getPreviousCommand().isEmpty());
        commandHistoryStub.setCursorPosition(-1);
        assertTrue(commandHistoryStub.getPreviousCommand().isEmpty());
    }

    @Test
    public void nextCommand_givesMatchingCommand() {
        // try one item
        commandHistoryStub = new CommandHistoryStub();
        // we need 2 items to proceed to the "next item"
        commandHistoryStub.add("find t/TA");
        commandHistoryStub.add("add n/Bobby");
        commandHistoryStub.setCursorPosition(0);
        assertTrue(commandHistoryStub.getNextCommand().isPresent());
        assertEquals(commandHistoryStub.getNextCommand().get(), "add n/Bobby");
    }

    @Test
    public void prevCommand_givesMatchingCommand() {
        // try one item
        commandHistoryStub = new CommandHistoryStub();
        // we need 2 items to proceed to the "previous item"
        commandHistoryStub.add("find t/TA");
        commandHistoryStub.add("add n/Bobby");
        commandHistoryStub.setCursorPosition(1);
        assertTrue(commandHistoryStub.getPreviousCommand().isPresent());
        assertEquals(commandHistoryStub.getPreviousCommand().get(), "find t/TA");

    }
}