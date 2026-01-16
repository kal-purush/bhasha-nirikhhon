package seedu.smartlib.logic.commands;

import static java.util.Objects.requireNonNull;

import seedu.smartlib.commons.core.Messages;
import seedu.smartlib.model.Model;
import seedu.smartlib.model.record.RecordContainsBookNamePredicate;

/**
 * Finds and lists all records in SmartLib whose name contains any of the argument keywords.
 * Keyword matching is case insensitive.
 */
public class FindRecordCommand extends Command {

    public static final String COMMAND_WORD = "findrecord";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Finds all records whose booknames contain any of "
            + "the specified keywords (case-insensitive) and displays them as a list with index numbers.\n"
            + "Parameters: KEYWORD [MORE_KEYWORDS]...\n"
            + "Example: " + COMMAND_WORD + "Harry Potter";

    private final RecordContainsBookNamePredicate predicate;

    /**
     * Creates an FindRecordCommand to find the specified record(s).
     *
     * @param predicate a Predicate used to find the specified record(s).
     */
    public FindRecordCommand(RecordContainsBookNamePredicate predicate) {
        this.predicate = predicate;
    }

    /**
     * Executes the command and returns the result message.
     *
     * @param model {@code Model} which the command should operate on.
     * @return feedback message of the operation result for display.
     */
    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        model.updateFilteredRecordList(predicate);
        return new CommandResult(
                String.format(Messages.MESSAGE_RECORD_LISTED_OVERVIEW, model.getFilteredRecordList().size()));
    }

    /**
     * Checks if this FindRecordCommand is equal to another FindRecordCommand.
     *
     * @param other the other FindRecordCommand to be compared.
     * @return true if this FindRecordCommand is equal to the other FindRecordCommand, and false otherwise.
     */
    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof FindRecordCommand // instanceof handles nulls
                && predicate.equals(((FindRecordCommand) other).predicate)); // state check
    }

}