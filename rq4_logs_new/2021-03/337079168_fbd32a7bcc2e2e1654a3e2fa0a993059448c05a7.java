package seedu.smartlib.logic.commands;

import static java.util.Objects.requireNonNull;

import seedu.smartlib.commons.core.Messages;
import seedu.smartlib.model.Model;
import seedu.smartlib.model.book.NameContainsKeywordsPredicate;

/**
 * Finds and lists all books in SmartLib whose name contains any of the argument keywords.
 * Keyword matching is case insensitive.
 */
public class FindBookCommand extends Command {

    public static final String COMMAND_WORD = "findbook";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Finds all books whose names contain any of "
            + "the specified keywords (case-insensitive) and displays them as a list with index numbers.\n"
            + "Parameters: KEYWORD [MORE_KEYWORDS]...\n"
            + "Example: " + COMMAND_WORD + "Harry Potter";

    private final NameContainsKeywordsPredicate predicate;

    public FindBookCommand(NameContainsKeywordsPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        model.updateFilteredBookList(predicate);
        return new CommandResult(
                String.format(Messages.MESSAGE_BOOKS_LISTED_OVERVIEW, model.getFilteredBookList().size()));
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof FindBookCommand // instanceof handles nulls
                && predicate.equals(((FindBookCommand) other).predicate)); // state check
    }
}