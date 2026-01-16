package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;

import java.util.List;

import seedu.address.commons.util.ToStringBuilder;
import seedu.address.logic.Messages;
import seedu.address.model.Model;
import seedu.address.model.person.OrdersContainsKeywordsPredicate;

/**
 * Finds and lists all persons in address book who ordered food contains any of the argument keywords.
 * Keyword matching is case-insensitive.
 */
public class FindOrdersCommand extends Command {

    public static final String COMMAND_WORD = "findOrders";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Finds all persons who ordered food contain any of "
            + "the specified keywords (case-insensitive) and displays them as a list with index numbers.\n"
            + "Parameters: KEYWORD [MORE_KEYWORDS]...\n"
            + "Example: " + COMMAND_WORD + " chickensoup noodles pizza";

    private final OrdersContainsKeywordsPredicate predicate;

    public FindOrdersCommand(OrdersContainsKeywordsPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        model.updateFilteredPersonList(predicate);
        return new CommandResult(
                String.format(Messages.MESSAGE_PERSONS_LISTED_OVERVIEW, model.getFilteredPersonList().size()));
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        // instanceof handles nulls
        if (!(other instanceof FindCommand)) {
            return false;
        }

        FindOrdersCommand otherFindCommand = (FindOrdersCommand) other;
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .add("predicate", predicate)
                .toString();
    }
}