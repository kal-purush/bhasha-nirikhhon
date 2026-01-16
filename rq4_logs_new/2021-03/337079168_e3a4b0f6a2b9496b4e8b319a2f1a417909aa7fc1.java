package seedu.smartlib.logic.commands;

import static java.util.Objects.requireNonNull;

import java.util.List;

import seedu.smartlib.commons.core.Messages;
import seedu.smartlib.commons.core.index.Index;
import seedu.smartlib.logic.commands.exceptions.CommandException;
import seedu.smartlib.model.Model;
import seedu.smartlib.model.book.Book;

/**
 * Deletes a book identified using it's displayed index from the SmarLib registered book base.
 */
public class DeleteBookCommand extends Command {

    public static final String COMMAND_WORD = "deletebook";

    public static final String MESSAGE_USAGE = COMMAND_WORD
            + ": Deletes the book identified by the index number used in the displayed book list.\n"
            + "Parameters: INDEX (must be a positive integer)\n"
            + "Example: " + COMMAND_WORD + " 1";

    public static final String MESSAGE_DELETE_READER_SUCCESS = "Deleted Book: %1$s";

    private final Index targetIndex;

    public DeleteBookCommand(Index targetIndex) {
        this.targetIndex = targetIndex;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);
        List<Book> lastShownList = model.getFilteredBookList();

        if (targetIndex.getZeroBased() >= lastShownList.size()) {
            throw new CommandException(Messages.MESSAGE_INVALID_READER_DISPLAYED_INDEX);
        }

        Book bookToDelete = lastShownList.get(targetIndex.getZeroBased());
        model.deleteBook(bookToDelete);
        return new CommandResult(String.format(MESSAGE_DELETE_READER_SUCCESS, bookToDelete));
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof DeleteBookCommand // instanceof handles nulls
                && targetIndex.equals(((DeleteBookCommand) other).targetIndex)); // state check
    }
}
