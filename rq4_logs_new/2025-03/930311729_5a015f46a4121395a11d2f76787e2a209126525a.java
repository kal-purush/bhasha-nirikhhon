package seedu.address.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.logic.commands.CommandTestUtil.assertCommandFailure;
import static seedu.address.logic.commands.CommandTestUtil.assertCommandSuccess;
import static seedu.address.testutil.Assert.assertThrows;
import static seedu.address.testutil.TypicalIndexes.INDEX_FIRST_PERSON;
import static seedu.address.testutil.TypicalIndexes.INDEX_SECOND_PERSON;
import static seedu.address.testutil.TypicalPersons.getTypicalAddressBook;

import org.junit.jupiter.api.Test;

import seedu.address.commons.core.index.Index;
import seedu.address.logic.Messages;
import seedu.address.model.AddressBook;
import seedu.address.model.Model;
import seedu.address.model.ModelManager;
import seedu.address.model.UserPrefs;
import seedu.address.model.person.Person;
import seedu.address.testutil.PersonBuilder;

/**
 * Contains integration tests (interaction with the Model) and unit tests for SavePreferenceCommand.
 */
public class SavePreferenceCommandTest {

    private Model model = new ModelManager(getTypicalAddressBook(), new UserPrefs());

    @Test
    public void constructor_nullIndex_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new SavePreferenceCommand(null, "no seafood"));
    }

    @Test
    public void constructor_nullPreference_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new SavePreferenceCommand(INDEX_FIRST_PERSON, null));
    }

    @Test
    public void constructor_nullIndexAndPreference_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new SavePreferenceCommand(null, null));
    }

    @Test
    public void toStringMethod() {
        SavePreferenceCommand savePreferenceCommand = new SavePreferenceCommand(INDEX_FIRST_PERSON, "no seafood");
        String expected = SavePreferenceCommand.class.getCanonicalName() + "{index="
                + savePreferenceCommand.index + ", preference="
                + savePreferenceCommand.preference + "}";
        assertEquals(expected, savePreferenceCommand.toString());
    }

    @Test
    public void execute_validIndexUnfilteredList_success() {
        Person personToEdit = model.getFilteredPersonList().get(INDEX_FIRST_PERSON.getZeroBased());
        Person editedPerson = new PersonBuilder(personToEdit).withPreference("no seafood").build();
        SavePreferenceCommand savePreferenceCommand = new SavePreferenceCommand(INDEX_FIRST_PERSON, "no seafood");

        String expectedMessage = String.format(SavePreferenceCommand.MESSAGE_SUCCESS, "no seafood", Messages.format(personToEdit));

        Model expectedModel = new ModelManager(new AddressBook(model.getAddressBook()), new UserPrefs());
        expectedModel.setPerson(personToEdit, editedPerson);

        assertCommandSuccess(savePreferenceCommand, model, expectedMessage, expectedModel);
    }

    @Test
    public void execute_invalidIndexUnfilteredList_throwsCommandException() {
        Index outOfBoundIndex = Index.fromOneBased(model.getFilteredPersonList().size() + 1);
        SavePreferenceCommand savePreferenceCommand = new SavePreferenceCommand(outOfBoundIndex, "no seafood");

        assertCommandFailure(savePreferenceCommand, model, SavePreferenceCommand.MESSAGE_INVALID_INDEX);
    }

    @Test
    public void equals() {
        final SavePreferenceCommand standardCommand = new SavePreferenceCommand(INDEX_FIRST_PERSON, "no seafood");
        final SavePreferenceCommand commandWithSameValues = new SavePreferenceCommand(INDEX_FIRST_PERSON, "no seafood");
        final SavePreferenceCommand commandWithDifferentIndex = new SavePreferenceCommand(INDEX_SECOND_PERSON, "no seafood");
        final SavePreferenceCommand commandWithDifferentPreference = new SavePreferenceCommand(INDEX_FIRST_PERSON, "no meat");

        // same values -> returns true
        assertTrue(standardCommand.equals(commandWithSameValues));

        // same object -> returns true
        assertTrue(standardCommand.equals(standardCommand));

        // null -> returns false
        assertFalse(standardCommand.equals(null));

        // different types -> returns false
        assertFalse(standardCommand.equals(new ClearCommand()));

        // different index -> returns false
        assertFalse(standardCommand.equals(commandWithDifferentIndex));

        // different preference -> returns false
        assertFalse(standardCommand.equals(commandWithDifferentPreference));
    }

}