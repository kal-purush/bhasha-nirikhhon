package seedu.address.logic.commands;

import seedu.address.model.Model;

/**
 * Format full feedback instructions for every command for display.
 */
public class FeedbackCommand extends Command {

    public static final String COMMAND_WORD = "feedback";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Shows program usage instructions.\n"
            + "Example: " + COMMAND_WORD;

    public static final String SHOWING_FEEDBACK_MESSAGE = "Opened feedback window.";

    @Override
    public CommandResult execute(Model model) {
        return new CommandResult(SHOWING_FEEDBACK_MESSAGE, false, false, true);
    }
}