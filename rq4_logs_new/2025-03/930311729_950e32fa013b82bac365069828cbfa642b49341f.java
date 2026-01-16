package seedu.address.logic.parser;

import static seedu.address.logic.Messages.MESSAGE_INVALID_COMMAND_FORMAT;
import static seedu.address.logic.parser.CommandParserTestUtil.assertParseFailure;
import static seedu.address.logic.parser.CommandParserTestUtil.assertParseSuccess;
import static seedu.address.testutil.TypicalIndexes.INDEX_FIRST_PERSON;

import org.junit.jupiter.api.Test;

import seedu.address.logic.commands.ViewOrdersCommand;

public class ViewOrdersCommandParserTest {

    private ViewOrdersCommandParser parser = new ViewOrdersCommandParser();

    /**
     * Tests if the parser correctly parses valid arguments
     * and returns a {@link ViewOrdersCommand} with the expected index.
     */
    @Test
    public void parse_validArgs_returnsViewOrdersCommand() {
        assertParseSuccess(parser, "1", new ViewOrdersCommand(INDEX_FIRST_PERSON));
    }

    @Test
    public void parse_invalidArgs_throwsParseException() {
        assertParseFailure(parser, "a", String.format(MESSAGE_INVALID_COMMAND_FORMAT, ViewOrdersCommand.MESSAGE_USAGE));
    }
}