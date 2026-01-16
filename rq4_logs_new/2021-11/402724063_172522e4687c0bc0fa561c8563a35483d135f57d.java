package seedu.academydirectory.commons.core;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class MessagesTest {
    @Test
    public void testHelpMessageGeneral() {
        String[] validCommands = {
            "add", "edit", "delete", "tag", "get",
            "grade", "attendance", "participation",
            "view", "show", "visualize", "filter", "sort",
            "list", "clear", "undo", "redo", "help",
            "exit", "history", "revert"
        };
        // test whether the help command contains all command on the user guide
        for (String command : validCommands) {
            assertTrue(Messages.GENERAL_HELP_MESSAGE.contains(command));
        }
    }
}