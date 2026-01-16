package seedu.smartlib.logic.parser;

import static seedu.smartlib.commons.core.Messages.MESSAGE_INVALID_COMMAND_FORMAT;
import static seedu.smartlib.logic.commands.CommandTestUtil.AUTHOR_DESC_HARRY;
import static seedu.smartlib.logic.commands.CommandTestUtil.AUTHOR_DESC_MAZE;
import static seedu.smartlib.logic.commands.CommandTestUtil.INVALID_AUTHOR_DESC;
import static seedu.smartlib.logic.commands.CommandTestUtil.INVALID_BOOKNAME_DESC;
import static seedu.smartlib.logic.commands.CommandTestUtil.INVALID_ISBN_DESC;
import static seedu.smartlib.logic.commands.CommandTestUtil.INVALID_PUBLISHER_DESC;
import static seedu.smartlib.logic.commands.CommandTestUtil.ISBN_DESC_HARRY;
import static seedu.smartlib.logic.commands.CommandTestUtil.ISBN_DESC_MAZE;
import static seedu.smartlib.logic.commands.CommandTestUtil.NAME_DESC_HARRY;
import static seedu.smartlib.logic.commands.CommandTestUtil.NAME_DESC_MAZE;
import static seedu.smartlib.logic.commands.CommandTestUtil.PREAMBLE_NON_EMPTY;
import static seedu.smartlib.logic.commands.CommandTestUtil.PREAMBLE_WHITESPACE;
import static seedu.smartlib.logic.commands.CommandTestUtil.PUBLISHER_DESC_HARRY;
import static seedu.smartlib.logic.commands.CommandTestUtil.PUBLISHER_DESC_MAZE;
import static seedu.smartlib.logic.commands.CommandTestUtil.VALID_AUTHOR_MAZE;
import static seedu.smartlib.logic.commands.CommandTestUtil.VALID_ISBN_MAZE;
import static seedu.smartlib.logic.commands.CommandTestUtil.VALID_NAME_MAZE;
import static seedu.smartlib.logic.commands.CommandTestUtil.VALID_PUBLISHER_MAZE;
import static seedu.smartlib.logic.parser.CommandParserTestUtil.assertParseFailure;
import static seedu.smartlib.logic.parser.CommandParserTestUtil.assertParseSuccess;
import static seedu.smartlib.testutil.TypicalModels.MAZE;

import org.junit.jupiter.api.Test;

import seedu.smartlib.commons.core.name.Name;
import seedu.smartlib.logic.commands.AddBookCommand;
import seedu.smartlib.model.book.Author;
import seedu.smartlib.model.book.Book;
import seedu.smartlib.model.book.Isbn;
import seedu.smartlib.model.book.Publisher;
import seedu.smartlib.testutil.BookBuilder;

public class AddBookCommandParserTest {
    private AddBookCommandParser parser = new AddBookCommandParser();

    @Test
    public void parse_allFieldsPresent_success() {
        Book expectedBook = new BookBuilder(MAZE).build();

        // whitespace only preamble
        assertParseSuccess(parser, PREAMBLE_WHITESPACE + NAME_DESC_MAZE + AUTHOR_DESC_MAZE
                + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE, new AddBookCommand(expectedBook));

        // multiple names - last name accepted
        assertParseSuccess(parser, NAME_DESC_HARRY + NAME_DESC_MAZE + AUTHOR_DESC_MAZE
                + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE, new AddBookCommand(expectedBook));

        // multiple authors - last author accepted
        assertParseSuccess(parser, NAME_DESC_MAZE + AUTHOR_DESC_HARRY + AUTHOR_DESC_MAZE
                + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE, new AddBookCommand(expectedBook));

        // multiple publishers - last publisher accepted
        assertParseSuccess(parser, NAME_DESC_MAZE + AUTHOR_DESC_MAZE + PUBLISHER_DESC_HARRY
                + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE, new AddBookCommand(expectedBook));

        // multiple isbn - last isbn accepted
        assertParseSuccess(parser, NAME_DESC_MAZE + AUTHOR_DESC_MAZE + PUBLISHER_DESC_MAZE
                + ISBN_DESC_HARRY + ISBN_DESC_MAZE, new AddBookCommand(expectedBook));
    }

    @Test
    public void parse_compulsoryFieldMissing_failure() {
        String expectedMessage = String.format(MESSAGE_INVALID_COMMAND_FORMAT, AddBookCommand.MESSAGE_USAGE);

        // missing name prefix
        assertParseFailure(parser, VALID_NAME_MAZE + AUTHOR_DESC_MAZE + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE,
                expectedMessage);

        // missing author prefix
        assertParseFailure(parser, NAME_DESC_MAZE + VALID_AUTHOR_MAZE + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE,
                expectedMessage);

        // missing publisher prefix
        assertParseFailure(parser, NAME_DESC_MAZE + AUTHOR_DESC_MAZE + VALID_PUBLISHER_MAZE + ISBN_DESC_MAZE,
                expectedMessage);

        // missing isbn prefix
        assertParseFailure(parser, NAME_DESC_MAZE + AUTHOR_DESC_MAZE + PUBLISHER_DESC_MAZE + VALID_ISBN_MAZE,
                expectedMessage);

        // all prefixes missing
        assertParseFailure(parser, VALID_NAME_MAZE + VALID_AUTHOR_MAZE + VALID_PUBLISHER_MAZE + VALID_ISBN_MAZE,
                expectedMessage);
    }

    @Test
    public void parse_invalidValue_failure() {
        // invalid name
        assertParseFailure(parser, INVALID_BOOKNAME_DESC + AUTHOR_DESC_MAZE
                + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE, Name.MESSAGE_CONSTRAINTS);

        // invalid author
        assertParseFailure(parser, NAME_DESC_MAZE + INVALID_AUTHOR_DESC
                + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE, Author.MESSAGE_CONSTRAINTS);

        // invalid publisher
        assertParseFailure(parser, NAME_DESC_MAZE + AUTHOR_DESC_MAZE
                + INVALID_PUBLISHER_DESC + ISBN_DESC_MAZE, Publisher.MESSAGE_CONSTRAINTS);

        // invalid isbn
        assertParseFailure(parser, NAME_DESC_MAZE + AUTHOR_DESC_MAZE
                + PUBLISHER_DESC_MAZE + INVALID_ISBN_DESC, Isbn.MESSAGE_CONSTRAINTS);

        // non-empty preamble
        assertParseFailure(parser, PREAMBLE_NON_EMPTY + NAME_DESC_MAZE + AUTHOR_DESC_MAZE
                        + PUBLISHER_DESC_MAZE + ISBN_DESC_MAZE,
                String.format(MESSAGE_INVALID_COMMAND_FORMAT, AddBookCommand.MESSAGE_USAGE));
    }
}
