package seedu.address.logic.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static seedu.address.testutil.TypicalPersons.getTypicalAddressBook;
import static seedu.address.testutil.TypicalIndexes.INDEX_FIRST_PERSON;

import org.junit.jupiter.api.Test;

import seedu.address.model.Model;
import seedu.address.model.ModelManager;
import seedu.address.model.UserPrefs;
import seedu.address.model.person.Person;

class AddOrderCommandTest {

    private final Model model = new ModelManager(getTypicalAddressBook(), new UserPrefs());

    @Test
    void execute_addOrder_success() throws Exception {
        Person personBefore = model.getFilteredPersonList().get(INDEX_FIRST_PERSON.getZeroBased());

        AddOrderCommand addOrderCommand = new AddOrderCommand(INDEX_FIRST_PERSON, "Chicken Rice");
        addOrderCommand.execute(model);

        Person personAfter = model.getFilteredPersonList().get(INDEX_FIRST_PERSON.getZeroBased());

        assertEquals(personBefore.getOrderHistory().getOrDefault("chicken rice", 0) + 1,
                personAfter.getOrderHistory().get("chicken rice")); // Order count increased
    }
}