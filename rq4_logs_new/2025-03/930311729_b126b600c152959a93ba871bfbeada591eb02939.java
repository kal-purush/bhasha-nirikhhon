package seedu.address.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.logic.Messages.MESSAGE_PERSONS_LISTED_OVERVIEW;
import static seedu.address.logic.commands.CommandTestUtil.assertCommandSuccess;
import static seedu.address.testutil.TypicalPersons.CARL;
import static seedu.address.testutil.TypicalPersons.ELLE;
import static seedu.address.testutil.TypicalPersons.FIONA;
import static seedu.address.testutil.TypicalPersons.getTypicalAddressBook;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import seedu.address.model.Model;
import seedu.address.model.ModelManager;
import seedu.address.model.UserPrefs;
import seedu.address.model.person.OrdersContainsKeywordsPredicate;

/**
 * Contains integration tests (interaction with the Model) for {@code FindOrdersCommand}.
 */
public class FindOrdersCommandTest {
    private Model model = new ModelManager(getTypicalAddressBook(), new UserPrefs());
    private Model expectedModel = new ModelManager(getTypicalAddressBook(), new UserPrefs());

    @Test
    public void equals() {
        OrdersContainsKeywordsPredicate firstPredicate =
                new OrdersContainsKeywordsPredicate(Collections.singletonList("soup"));
        OrdersContainsKeywordsPredicate secondPredicate =
                new OrdersContainsKeywordsPredicate(Collections.singletonList("rice"));

        FindOrdersCommand findFirstCommand = new FindOrdersCommand(firstPredicate);
        FindOrdersCommand findSecondCommand = new FindOrdersCommand(secondPredicate);

        // same object -> returns true
        assertTrue(findFirstCommand.equals(findFirstCommand));

        // same values -> returns true
        FindOrdersCommand findFirstCommandCopy = new FindOrdersCommand(firstPredicate);
        assertTrue(findFirstCommand.equals(findFirstCommandCopy));

        // different types -> returns false
        assertFalse(findFirstCommand.equals(1));

        // null -> returns false
        assertFalse(findFirstCommand.equals(null));

        // different person -> returns false
        assertFalse(findFirstCommand.equals(findSecondCommand));
    }

    @Test
    public void execute_zeroKeywords_noPersonFound() {
        String expectedMessage = String.format(MESSAGE_PERSONS_LISTED_OVERVIEW, 0);
        OrdersContainsKeywordsPredicate predicate = preparePredicate("");
        FindOrdersCommand command = new FindOrdersCommand(predicate);

        expectedModel.updateFilteredPersonList(predicate);
        assertCommandSuccess(command, model, expectedMessage, expectedModel);
        assertEquals(Collections.emptyList(), model.getFilteredPersonList());
    }

    @Test
    public void execute_multipleKeywords_multiplePersonsFound() {
        String expectedMessage = String.format(MESSAGE_PERSONS_LISTED_OVERVIEW, 3);
        OrdersContainsKeywordsPredicate predicate = preparePredicate("milo chickenrice pizza");
        FindOrdersCommand command = new FindOrdersCommand(predicate);

        expectedModel.updateFilteredPersonList(predicate);
        assertCommandSuccess(command, model, expectedMessage, expectedModel);
        assertEquals(Arrays.asList(CARL, ELLE, FIONA), model.getFilteredPersonList());
    }

    @Test
    public void toStringMethod() {
        OrdersContainsKeywordsPredicate predicate = new OrdersContainsKeywordsPredicate(Arrays.asList("keyword"));
        FindOrdersCommand findOrdersCommand = new FindOrdersCommand(predicate);
        String expected = FindOrdersCommand.class.getCanonicalName() + "{predicate=" + predicate + "}";
        assertEquals(expected, findOrdersCommand.toString());
    }

    /**
     * Parses {@code userInput} into a {@code OrdersContainsKeywordsPredicate}.
     */
    private OrdersContainsKeywordsPredicate preparePredicate(String userInput) {
        return new OrdersContainsKeywordsPredicate(Arrays.asList(userInput.split("\\s+")));
    }
}