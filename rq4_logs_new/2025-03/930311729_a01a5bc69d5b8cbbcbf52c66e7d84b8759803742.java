package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;
import static seedu.address.logic.Messages.MESSAGE_PERSONS_LISTED_OVERVIEW;

import seedu.address.model.Model;
import seedu.address.model.person.TagContainsKeywordPredicate;

public class FindTagCommand extends Command {

    public static final String COMMAND_WORD = "findTag";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Finds all customers with the specified tag.\n"
            + "Example: " + COMMAND_WORD + " TAG (Allowed values: VIP, Regular, New)";

    public static final String MESSAGE_SUCCESS = "Customers with tag '%s':\n%s";

    private final TagContainsKeywordPredicate predicate;


    public FindTagCommand(TagContainsKeywordPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        model.updateFilteredPersonList(predicate);
        return new CommandResult(
                String.format(MESSAGE_PERSONS_LISTED_OVERVIEW, model.getFilteredPersonList().size()));
    }

    @Override
    public boolean equals(Object other) {
        return other == this
                || (other instanceof FindTagCommand
                && predicate.equals(((FindTagCommand) other).predicate));
    }
}