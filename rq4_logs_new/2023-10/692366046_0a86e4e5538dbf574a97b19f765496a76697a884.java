package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;

import seedu.address.model.Model;

/**
 * Sorts all persons in the address book by name lexicographically.
 */
public class SortNameCommand extends Command {

    public static final String COMMAND_WORD = "sort-name";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Sorts all contacts in NUSCourseMates by name.\n"
            + "Example: " + COMMAND_WORD;

    public static final String MESSAGE_SUCCESS = "Sorted all contacts by name";

    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        model.sortPersonList();
        return new CommandResult(MESSAGE_SUCCESS);
    }

}