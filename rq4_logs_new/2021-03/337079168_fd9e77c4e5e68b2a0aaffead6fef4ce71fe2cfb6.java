package seedu.smartlib.logic.parser;

import seedu.smartlib.commons.core.name.Name;
import seedu.smartlib.logic.commands.BorrowCommand;
import seedu.smartlib.logic.parser.exceptions.ParseException;
import seedu.smartlib.model.record.DateBorrowed;
import seedu.smartlib.model.record.Record;

import java.time.LocalDate;

import static java.util.Objects.requireNonNull;
import static seedu.smartlib.logic.parser.CliSyntax.PREFIX_BOOK;
import static seedu.smartlib.logic.parser.CliSyntax.PREFIX_READER;

/**
 * Parses input arguments and creates a new {@code BorrowCommand} object
 */
public class ReturnCommandParser implements Parser<BorrowCommand> {
    /**
     * Parses the given {@code String} of arguments in the context of the {@code BorrowCommand}
     * and returns a {@code BorrowCommand} object for execution.
     * @throws ParseException if the user input does not conform the expected format
     */
    public BorrowCommand parse(String args) throws ParseException {
        requireNonNull(args);
        ArgumentMultimap argMultimap = ArgumentTokenizer.tokenize(args, PREFIX_BOOK, PREFIX_READER);


        Name bookName = ParserUtil.parseName(argMultimap.getValue(PREFIX_BOOK).get());
        Name readerName = ParserUtil.parseName(argMultimap.getValue(PREFIX_READER).get());
        DateBorrowed dateBorrowed = new DateBorrowed(LocalDate.now());
        Record record = new Record(bookName, readerName, dateBorrowed);

        return new BorrowCommand(record);
    }
}