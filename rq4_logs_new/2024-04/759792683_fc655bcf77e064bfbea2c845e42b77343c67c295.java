package seedu.address.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.logic.commands.CommandTestUtil.assertCommandSuccess;
import static seedu.address.logic.commands.FilterCommand.MESSAGE_FILTER_ADDRESS_BOOK_SUCCESS;
import static seedu.address.testutil.TypicalPersons.ALICE;
import static seedu.address.testutil.TypicalPersons.BENSON;
import static seedu.address.testutil.TypicalPersons.CARL;
import static seedu.address.testutil.TypicalPersons.DANIEL;
import static seedu.address.testutil.TypicalPersons.ELLE;
import static seedu.address.testutil.TypicalPersons.FIONA;
import static seedu.address.testutil.TypicalPersons.GEORGE;
import static seedu.address.testutil.TypicalPersons.getTypicalAddressBook;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import seedu.address.logic.Messages;
import seedu.address.model.CommandHistory;
import seedu.address.model.Model;
import seedu.address.model.ModelManager;
import seedu.address.model.UserPrefs;
import seedu.address.model.person.*;


/**
 * Contains integration tests (interaction with the Model) for {@code PaymentCommand}.
 */
public class PaymentCommandTest {
    private Model model = new ModelManager(getTypicalAddressBook(), new UserPrefs(), new CommandHistory());
    private Model expectedModel = new ModelManager(getTypicalAddressBook(), new UserPrefs(), new CommandHistory());

    @Test
    public void equals() {
        PaymentFilterPredicate firstPredicate =
                new PaymentFilterPredicate(new Payment());
        PaymentFilterPredicate secondPredicate =
                new PaymentFilterPredicate(new Payment("Paid"));

        PaymentCommand PaymentFirstCommand = new PaymentCommand(firstPredicate);
        PaymentCommand PaymentSecondCommand = new PaymentCommand(secondPredicate);

        // same object -> returns true
        assertTrue(PaymentFirstCommand.equals(PaymentFirstCommand));

        // same values -> returns true
        PaymentCommand findFirstCommandCopy = new PaymentCommand(firstPredicate);
        assertTrue(PaymentFirstCommand.equals(findFirstCommandCopy));

        // different types -> returns false
        assertFalse(PaymentFirstCommand.equals(1));

        // null -> returns false
        assertFalse(PaymentFirstCommand.equals(null));

        // different person -> returns false
        assertFalse(PaymentFirstCommand.equals(PaymentSecondCommand));
    }

    @Test
    public void execute_filterPayment_noStudentFound() {
        PaymentFilterPredicate predicate =
                new PaymentFilterPredicate(new Payment("Not Paid"));
        String expectedMessage =
                String.format(MESSAGE_FILTER_ADDRESS_BOOK_SUCCESS + Messages.MESSAGE_PERSONS_LISTED_OVERVIEW,
                        0, predicate.filterResult());
        PaymentCommand command = new PaymentCommand(predicate);
        expectedModel.updateFilteredPersonList(predicate);
        assertCommandSuccess(command, model, expectedMessage, expectedModel);
        assertEquals(Collections.emptyList(), model.getFilteredPersonList());
    }

    @Test
    public void execute_filterPayment_multipleStudentsFound() {
        PaymentFilterPredicate predicate =
                new PaymentFilterPredicate(new Payment("Paid"));
        String expectedMessage =
                String.format(MESSAGE_FILTER_ADDRESS_BOOK_SUCCESS + Messages.MESSAGE_PERSONS_LISTED_OVERVIEW,
                        7, predicate.filterResult());
        PaymentCommand command = new PaymentCommand(predicate);
        expectedModel.updateFilteredPersonList(predicate);
        assertCommandSuccess(command, model, expectedMessage, expectedModel);
        assertEquals(Arrays.asList(ALICE, BENSON, CARL, DANIEL, ELLE, FIONA, GEORGE), model.getFilteredPersonList());
    }

    @Test
    public void toStringMethod() {
        PaymentFilterPredicate predicate = new PaymentFilterPredicate(new Payment());
        PaymentCommand filterCommand = new PaymentCommand(predicate);
        String expected = PaymentCommand.class.getSimpleName() + "{predicate=" + predicate + "}";
        assertEquals(expected, filterCommand.toString());
    }
}