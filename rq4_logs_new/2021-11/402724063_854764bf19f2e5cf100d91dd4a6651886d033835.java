package seedu.academydirectory.ui;

import seedu.academydirectory.logic.commands.CommandResult;
import seedu.academydirectory.logic.commands.exceptions.CommandException;
import seedu.academydirectory.logic.parser.exceptions.ParseException;

/**
 * Represents a function that can execute commands.
 */
@FunctionalInterface
public interface CommandExecutor {
    /**
     * Executes the command and returns the result.
     *
     * @see seedu.academydirectory.logic.Logic#execute(String)
     */
    CommandResult execute(String commandText) throws CommandException, ParseException;
}