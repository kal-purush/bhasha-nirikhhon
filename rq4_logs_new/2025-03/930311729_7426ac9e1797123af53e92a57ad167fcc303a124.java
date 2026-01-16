package seedu.address.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.logic.commands.CommandTestUtil.assertCommandSuccess;
import static seedu.address.testutil.TypicalPersons.BENSON;
import static seedu.address.testutil.TypicalPersons.getTypicalAddressBook;

import java.util.*;

import org.junit.jupiter.api.Test;

import seedu.address.model.Model;
import seedu.address.model.ModelManager;
import seedu.address.model.UserPrefs;
import seedu.address.model.person.*;

/**
 * Contains integration tests (interaction with the Model) for {@code FindPreferencesCommand}.
 */
public class FindPreferencesCommandTest {

    private Model model = new ModelManager(getTypicalAddressBook(), new UserPrefs());
    private Model expectedModel = new ModelManager(getTypicalAddressBook(), new UserPrefs());

    @Test
    public void execute_singleKeyword_singleMatch() {
        String expectedMessage = String.format(FindPreferencesCommand.MESSAGE_SUCCESS, 1);
        PreferenceContainsKeywordPredicate predicate = new PreferenceContainsKeywordPredicate("no seafood");
        FindPreferencesCommand command = new FindPreferencesCommand(predicate);

        expectedModel.updateFilteredPersonList(predicate);
        assertCommandSuccess(command, model, expectedMessage, expectedModel);
        assertEquals(Arrays.asList(BENSON), model.getFilteredPersonList());
    }

    @Test
    public void execute_noMatchingPreferences_noResults() {
        String expectedMessage = String.format(FindPreferencesCommand.MESSAGE_SUCCESS, 0);
        PreferenceContainsKeywordPredicate predicate = new PreferenceContainsKeywordPredicate("keto");
        FindPreferencesCommand command = new FindPreferencesCommand(predicate);

        expectedModel.updateFilteredPersonList(predicate);
        assertCommandSuccess(command, model, expectedMessage, expectedModel);
        assertEquals(Collections.emptyList(), model.getFilteredPersonList());
    }

    @Test
    public void execute_caseInsensitiveSearch_matchesCorrectly() {
        String expectedMessage = String.format(FindPreferencesCommand.MESSAGE_SUCCESS, 1);
        PreferenceContainsKeywordPredicate predicate = new PreferenceContainsKeywordPredicate("NO SEAFOOD");
        FindPreferencesCommand command = new FindPreferencesCommand(predicate);

        expectedModel.updateFilteredPersonList(predicate);
        assertCommandSuccess(command, model, expectedMessage, expectedModel);
        assertEquals(Arrays.asList(BENSON), model.getFilteredPersonList());
    }

    @Test
    public void equals() {
        PreferenceContainsKeywordPredicate firstPredicate =
                new PreferenceContainsKeywordPredicate("first");
        PreferenceContainsKeywordPredicate secondPredicate =
                new PreferenceContainsKeywordPredicate("second");

        FindPreferencesCommand findPreferencesFirstCommand = new FindPreferencesCommand(firstPredicate);
        FindPreferencesCommand findPreferencesSecondCommand = new FindPreferencesCommand(secondPredicate);

        // same object -> returns true
        assertTrue(findPreferencesFirstCommand.equals(findPreferencesFirstCommand));

        // same values -> returns true
        FindPreferencesCommand findPreferencesFirstCommandCopy = new FindPreferencesCommand(firstPredicate);
        assertTrue(findPreferencesFirstCommand.equals(findPreferencesFirstCommandCopy));

        // different types -> returns false
        assertFalse(findPreferencesFirstCommand.equals(1));

        // null -> returns false
        assertFalse(findPreferencesFirstCommand.equals(null));

        // different person -> returns false
        assertFalse(findPreferencesFirstCommand.equals(findPreferencesSecondCommand));
    }
}