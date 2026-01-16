package seedu.smartlib.logic.commands;

import static java.util.Objects.requireNonNull;
import static seedu.smartlib.logic.parser.CliSyntax.PREFIX_BOOK;
import static seedu.smartlib.logic.parser.CliSyntax.PREFIX_AUTHOR;
import static seedu.smartlib.logic.parser.CliSyntax.PREFIX_PUBLISHER;
import static seedu.smartlib.logic.parser.CliSyntax.PREFIX_ISBN;

import seedu.smartlib.logic.commands.exceptions.CommandException;
import seedu.smartlib.model.Model;
import seedu.smartlib.model.book.Book;

/**
 * Adds a reader to the registered reader base.
 */
public class AddBookCommand extends Command {

    public static final String COMMAND_WORD = "addbook";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Adds a book to the booklist. "
            + "Parameters: "
            + PREFIX_BOOK + "BOOKNAME "
            + PREFIX_AUTHOR + "AUTHOR "
            + PREFIX_PUBLISHER + "PUBLISHER "
            + PREFIX_ISBN + "ISBN ";

    public static final String MESSAGE_SUCCESS = "New book added: %1$s";
    public static final String MESSAGE_DUPLICATE_BOOK = "This book already exists in the book base";

    private final Book toAdd;

    /**
     * Creates an AddCommand to add the specified {@code Person}
     */
    public AddBookCommand(Book book) {
        requireNonNull(book);
        toAdd = book;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);

        if (model.hasBook(toAdd)) {
            throw new CommandException(MESSAGE_DUPLICATE_BOOK);
        }

        model.addBook(toAdd);
        return new CommandResult(String.format(MESSAGE_SUCCESS, toAdd));
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof AddBookCommand // instanceof handles nulls
                && toAdd.equals(((AddBookCommand) other).toAdd));
    }
}