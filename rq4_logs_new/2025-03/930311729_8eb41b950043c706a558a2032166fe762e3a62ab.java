package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;

import java.util.function.Predicate;

import seedu.address.model.Model;
import seedu.address.model.person.Person;

/**
 * Finds and lists all persons in address book who have a certain preference.
 */
public class FindPreferencesCommand extends Command {
    public static final String COMMAND_WORD = "findPreferences";

    public static final String MESSAGE_USAGE = COMMAND_WORD
            + ": Finds all persons with the specified preference (case-sensitive) "
            + "and displays them as a list with index numbers.\n"
            + "Parameters: PREFERENCE\n"
            + "Example: " + COMMAND_WORD + " Vegetarian";

    private final Predicate<Person> predicate;

    public FindPreferencesCommand(Predicate<Person> predicate) {
        this.predicate = predicate;
    }

    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        model.updateFilteredPersonList(predicate);
        return new CommandResult(
                String.format("%d persons listed!", model.getFilteredPersonList().size()));
    }

    @Override
    public boolean equals(Object other) {
        return other == this
                || (other instanceof FindPreferencesCommand
                && predicate.equals(((FindPreferencesCommand) other).predicate));
    }
}