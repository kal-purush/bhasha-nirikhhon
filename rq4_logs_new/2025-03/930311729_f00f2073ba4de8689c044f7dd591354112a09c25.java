package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;
import static seedu.address.logic.parser.CliSyntax.PREFIX_DISH;

import java.util.List;

import seedu.address.commons.core.index.Index;
import seedu.address.logic.Messages;
import seedu.address.logic.commands.exceptions.CommandException;
import seedu.address.model.Model;
import seedu.address.model.person.Person;

/**
 * Represents a command that adds an order to a customer's order history.
 */
public class AddOrderCommand extends Command {

    public static final String COMMAND_WORD = "addOrder";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Adds an order to the customer’s order history. "
            + "Parameters: INDEX (must be a positive integer) "
            + PREFIX_DISH + "DISH_NAME\n"
            + "Example: " + COMMAND_WORD + " 1 " + PREFIX_DISH + "Chicken Rice";

    public static final String MESSAGE_SUCCESS = "Added order: %s to %s";
    public static final String MESSAGE_INVALID_PERSON = "The person index provided is invalid.";

    private final Index index;
    private final String dishName;

    /**
     * Constructs an {@code AddOrderCommand}.
     *
     * @param index    The index of the customer in the filtered person list.
     * @param dishName The name of the dish to be added to the order history.
     */
    public AddOrderCommand(Index index, String dishName) {
        requireNonNull(index);
        requireNonNull(dishName);
        this.index = index;
        this.dishName = dishName;
    }

    /**
     * Executes the AddOrderCommand, updating the customer's order history.
     *
     * @param model {@code Model} containing the application's data.
     * @return A {@code CommandResult} indicating the outcome of the operation.
     * @throws CommandException If the specified customer index is invalid.
     */
    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);
        List<Person> lastShownList = model.getFilteredPersonList();

        if (index.getZeroBased() >= lastShownList.size()) {
            throw new CommandException(Messages.MESSAGE_INVALID_PERSON_DISPLAYED_INDEX);
        }

        Person customer = lastShownList.get(index.getZeroBased());

        // Create a new Person object with the same details but updated order history
        Person updatedCustomer = new Person(customer);
        updatedCustomer.addOrder(dishName); // Add the order to the copied object

        // Replace the old person with the updated one
        model.setPerson(customer, updatedCustomer);

        return new CommandResult(String.format(MESSAGE_SUCCESS, dishName, updatedCustomer.getName()));
    }

}
