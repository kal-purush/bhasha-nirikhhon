package seedu.address.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.testutil.TypicalIndexes.INDEX_FIRST_PERSON;
import static seedu.address.testutil.TypicalIndexes.INDEX_SECOND_PERSON;
import static seedu.address.testutil.TypicalPersons.getTypicalAddressBook;

import org.junit.jupiter.api.Test;

import seedu.address.commons.core.index.Index;
import seedu.address.model.CommandHistory;
import seedu.address.model.Model;
import seedu.address.model.ModelManager;
import seedu.address.model.UserPrefs;


/**
 * Contains integration tests (interaction with the Model) and unit tests for
 * {@code HistoryCommand}.
 */
public class HistoryCommandTest {

    private Model model = new ModelManager(getTypicalAddressBook(), new UserPrefs(), new CommandHistory());

    @Test
    public void equals() {
        HistoryCommand historyFirstCommand = new HistoryCommand(INDEX_FIRST_PERSON);
        HistoryCommand historySecondCommand = new HistoryCommand(INDEX_SECOND_PERSON);

        // same object -> returns true
        assertTrue(historyFirstCommand.equals(historyFirstCommand));

        // same values -> returns true
        HistoryCommand historyFirstCommandCopy = new HistoryCommand(INDEX_FIRST_PERSON);
        assertTrue(historyFirstCommand.equals(historyFirstCommandCopy));

        // different types -> returns false
        assertFalse(historyFirstCommand.equals(1));

        // null -> returns false
        assertFalse(historyFirstCommand.equals(null));

        // different indexes -> returns false
        assertFalse(historyFirstCommand.equals(historySecondCommand));
    }

    @Test
    public void toStringMethod() {
        Index targetIndex = Index.fromOneBased(1);
        HistoryCommand historyCommand = new HistoryCommand(targetIndex);
        String expected = HistoryCommand.class.getCanonicalName() + "{targetIndex=" + targetIndex + "}";
        assertEquals(expected, historyCommand.toString());
    }
}