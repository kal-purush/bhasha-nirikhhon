package seedu.smartlib.logic.commands;

import seedu.smartlib.logic.commands.exceptions.CommandException;
import seedu.smartlib.model.Model;
import seedu.smartlib.model.record.Record;
import seedu.smartlib.model.record.exceptions.RecordNotFoundException;

import static seedu.smartlib.commons.util.CollectionUtil.requireAllNonNull;
import static seedu.smartlib.logic.parser.CliSyntax.PREFIX_BOOK;

/**
 * Marks the record indicating that a reader returning a book
 */
public class ReturnCommand extends Command {

    public static final String COMMAND_WORD = "return";
    public static final String MESSAGE_USAGE = COMMAND_WORD + ": returns the book by the reader, both specified by name";
    public static final String MESSAGE_SUCCESS = "Record marked as returned.";
    public static final String MESSAGE_NO_SUCH_RECORD_FOUND = "No such record found. Either already returned or never borrowed";

    private final Record toReturn;

    /**
     * Creates a ReturnCommand to add a record
     * @param record recordToAdd
     */
    public ReturnCommand(Record record) {
        requireAllNonNull(record);
        toReturn = record;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireAllNonNull(model);

        if (!model.hasRecord(toReturn)) {
            return new CommandResult(String.format(MESSAGE_NO_SUCH_RECORD_FOUND, toReturn));
        }

        model.markRecordAsReturned(toReturn);
        return new CommandResult(String.format(MESSAGE_SUCCESS, toReturn));
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof ReturnCommand // instanceof handles nulls
                && toReturn.equals(((ReturnCommand) other).toReturn));
    }
}