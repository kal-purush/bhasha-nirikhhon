package seedu.academydirectory.logic.parser;

import seedu.academydirectory.commons.core.Messages;
import seedu.academydirectory.logic.commands.ClearCommand;
import seedu.academydirectory.logic.commands.Command;
import seedu.academydirectory.logic.commands.ExitCommand;
import seedu.academydirectory.logic.commands.HistoryCommand;
import seedu.academydirectory.logic.commands.ListCommand;
import seedu.academydirectory.logic.commands.RedoCommand;
import seedu.academydirectory.logic.commands.UndoCommand;
import seedu.academydirectory.logic.commands.VisualizeCommand;
import seedu.academydirectory.logic.parser.exceptions.ParseException;

public class SingularCommandParser implements Parser<Command> {

    public static final String MESSAGE_NO_ARGUMENT_SHOULD_FOLLOW = "Invalid usage detected. No other keyword " +
            "should follow the command %1$s";

    private final String commandWord;

    /**
     * Constructor for a Singular Command parser
     * @param commandWord singular command
     */
    public SingularCommandParser(String commandWord) {
        this.commandWord = commandWord;
    }

    /**
     * parse user command based on the user input created
     * @param userInput inputs
     * @return corresponding command
     * @throws ParseException if command is not valid
     */
    @Override
    public Command parse(String userInput) throws ParseException {
        assertSingularCommand(userInput);
        switch (commandWord) {
        case ClearCommand.COMMAND_WORD:
            return new ClearCommand();
        case ExitCommand.COMMAND_WORD:
            return new ExitCommand();
        case HistoryCommand.COMMAND_WORD:
            return new HistoryCommand();
        case ListCommand.COMMAND_WORD:
            return new ListCommand();
        case RedoCommand.COMMAND_WORD:
            return new RedoCommand();
        case UndoCommand.COMMAND_WORD:
            return new UndoCommand();
        case VisualizeCommand.COMMAND_WORD:
            return new VisualizeCommand();
        default:
            throw new ParseException(Messages.MESSAGE_UNKNOWN_COMMAND);
        }
    }

    /**
     * assert that there is no other argument in the keywords
     * @param arguments arguments
     * @throws ParseException if the arguments are non-empty
     */
    private void assertSingularCommand(String arguments) throws ParseException {
        if (!arguments.isEmpty()) {
            throw new ParseException(String.format(MESSAGE_NO_ARGUMENT_SHOULD_FOLLOW, commandWord));
        }
    }
}