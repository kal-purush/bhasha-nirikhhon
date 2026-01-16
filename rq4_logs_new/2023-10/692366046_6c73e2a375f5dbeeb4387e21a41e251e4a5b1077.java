package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;

import seedu.address.model.Model;
import seedu.address.model.person.sorter.PersonSorter;

/**
 * Sorts all persons in the address book based on a specified sorting criterion.
 */
public class SortCommand extends Command {

    public static final String COMMAND_WORD = "sort";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Sorts all contacts in NUSCourseMates "
            + "according to the specified sort criterion.\n"
            + "Parameters: SORT_CRITERION \n"
            + "Example: " + COMMAND_WORD + " name-ascending";

    public static final String MESSAGE_SUCCESS = "Sorted all contacts";

    private final PersonSorter sorter;

    public SortCommand(PersonSorter sorter) {
        this.sorter = sorter;
    }

    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        model.sortPersonList(sorter);
        return new CommandResult(MESSAGE_SUCCESS);
    }
}