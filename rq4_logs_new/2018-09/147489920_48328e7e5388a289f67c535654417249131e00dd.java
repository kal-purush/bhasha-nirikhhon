package systemtests;

import static seedu.address.commons.core.Messages.MESSAGE_UNKNOWN_COMMAND;
import static seedu.address.logic.commands.SuggestCommand.MESSAGE_SUGGEST_COMMAND_SUCCESS;

import org.junit.Test;

import seedu.address.logic.commands.SuggestCommand;
import seedu.address.model.Model;

public class SuggestCommandSystemTest extends AddressBookSystemTest {

    @Test
    public void suggestCommand() throws Exception {
        assertSuggestCommandSuccessBehavior("a", "add");
        assertSuggestCommandSuccessBehavior("c", "clear");
        assertSuggestCommandSuccessBehavior("d", "delete");
        assertSuggestCommandSuccessBehavior("e", "edit", "exit");
        assertSuggestCommandSuccessBehavior("f", "find");
        assertSuggestCommandSuccessBehavior("h", "help", "history");
        assertSuggestCommandSuccessBehavior("l", "list");
        assertSuggestCommandSuccessBehavior("r", "redo");
        assertSuggestCommandSuccessBehavior("s", "select");
        assertSuggestCommandSuccessBehavior("u", "undo");

        assertSuggestCommandFailureBehavior("b");
        assertSuggestCommandFailureBehavior("g");
        assertSuggestCommandFailureBehavior("i");
        assertSuggestCommandFailureBehavior("j");
        assertSuggestCommandFailureBehavior("k");
        assertSuggestCommandFailureBehavior("m");
        assertSuggestCommandFailureBehavior("n");
        assertSuggestCommandFailureBehavior("o");
        assertSuggestCommandFailureBehavior("p");
        assertSuggestCommandFailureBehavior("q");
        assertSuggestCommandFailureBehavior("t");
        assertSuggestCommandFailureBehavior("v");
        assertSuggestCommandFailureBehavior("w");
        assertSuggestCommandFailureBehavior("x");
        assertSuggestCommandFailureBehavior("y");
        assertSuggestCommandFailureBehavior("z");
    }

    /**
     * Performs UI visible verification of suggest command behavior resulting in 1 or more suggested commands.
     * Logic tests are done by SuggestCommandTest
     * @see seedu.address.logic.commands.SuggestCommandTest
     */
    private void assertSuggestCommandSuccessBehavior(String command, String... commandWords) throws Exception {
        String expectedResultMessage = String.format(MESSAGE_SUGGEST_COMMAND_SUCCESS,
                SuggestCommand.combineCommandWords(commandWords));
        String expectedCommandBoxText;
        if (commandWords.length == 1) {
            expectedCommandBoxText = commandWords[0];
        } else if (commandWords.length > 1) {
            expectedCommandBoxText = "";
        } else {
            throw new Exception("Wrong use of assertCommandSuccess!");
        }
        Model expectedModel = getModel();
        executeCommand(command);
        assertApplicationDisplaysExpected(expectedCommandBoxText, expectedResultMessage, expectedModel);
        assertCommandBoxShowsDefaultStyle();
    }

    /**
     * Performs UI visible verification of suggest command behavior resulting in no suggested commands.
     * Logic tests are done by SuggestCommandTest
     * @see seedu.address.logic.commands.SuggestCommandTest
     */
    private void assertSuggestCommandFailureBehavior(String command) {
        String expectedResultMessage = MESSAGE_UNKNOWN_COMMAND;
        String expectedCommandBoxText = command;
        Model expectedModel = getModel();
        executeCommand(command);
        assertApplicationDisplaysExpected(expectedCommandBoxText, expectedResultMessage, expectedModel);
        assertCommandBoxShowsErrorStyle();
    }
}