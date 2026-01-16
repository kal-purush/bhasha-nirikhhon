package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;

import seedu.address.model.Model;

/**
 * Sorts all persons in the address book by number of courses taken.
 */
public class SortCourseCommand extends Command {

    public static final String COMMAND_WORD = "sort-course";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Sorts all contacts in NUSCourseMates "
            + "by number of courses taken.\n"
            + "Example: " + COMMAND_WORD;

    public static final String MESSAGE_SUCCESS = "Sorted all contacts by course";

    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        model.sortPersonListByCourse();
        return new CommandResult(MESSAGE_SUCCESS);
    }

}