package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;
import java.util.List;
import java.util.Set;

import seedu.address.commons.core.index.Index;
import seedu.address.commons.util.ToStringBuilder;
import seedu.address.logic.Messages;
import seedu.address.logic.commands.exceptions.CommandException;
import seedu.address.model.Model;
import seedu.address.model.person.Person;

/**
 * Views past orders of a person identified using it's displayed index from the address book.
 */
public class ViewOrdersCommand extends Command {

    public static final String COMMAND_WORD = "viewOrders";

    public static final String MESSAGE_USAGE = COMMAND_WORD
            + ": Views the past orders of a person identified by the index number used in the displayed person list.\n"
            + "Parameters: INDEX (must be a positive integer)\n"
            + "Example: " + COMMAND_WORD + " 1";

    public static final String MESSAGE_VIEWORDERS_SUCCESS = "Past Orders for %1$s:\n";

    private final Index targetIndex;

    public ViewOrdersCommand(Index targetIndex) {
        requireNonNull(targetIndex);
        this.targetIndex = targetIndex;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);
        List<Person> lastShownList = model.getFilteredPersonList();

        if (targetIndex.getZeroBased() >= lastShownList.size()) {
            throw new CommandException(Messages.MESSAGE_INVALID_PERSON_DISPLAYED_INDEX);
        }

        Person personToView = lastShownList.get(targetIndex.getZeroBased());
        Set<String> orderNames = personToView.getOrderHistory().keySet();
        String message = String.format(MESSAGE_VIEWORDERS_SUCCESS, Messages.format(personToView));

        if (orderNames.isEmpty()) {
            return new CommandResult(message + "No past orders.");
        }

        String messageWithOrders = message + String.join(",",personToView.getOrderHistory().keySet());

        return new CommandResult(messageWithOrders);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        // instanceof handles nulls
        if (!(other instanceof DeleteCommand)) {
            return false;
        }

        ViewOrdersCommand otherViewOrdersCommand = (ViewOrdersCommand) other;
        return targetIndex.equals(otherViewOrdersCommand.targetIndex);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .add("targetIndex", targetIndex)
                .toString();
    }
}