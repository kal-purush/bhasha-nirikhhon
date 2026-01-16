package seedu.address.logic.commands;

import seedu.address.commons.core.index.Index;
import seedu.address.logic.Messages;
import seedu.address.logic.commands.exceptions.CommandException;
import seedu.address.model.Model;
import seedu.address.model.person.Person;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static seedu.address.commons.util.CollectionUtil.requireAllNonNull;

public class SavePreferenceCommand extends Command {

    public static final String COMMAND_WORD = "savePreference";

    public static final String MESSAGE_USAGE = COMMAND_WORD
            + ": Adds the preference to the customer identified "
            + "by the index number used in the last person listing.\n"
            + "Parameters: INDEX (must be a positive integer) "
            + "s/ [PREFERENCE]\n"
            + "Example: " + COMMAND_WORD + " 1 "
            + "s/no seafood";

    public static final String MESSAGE_SUCCESS = "Added %s to %s";
    public static final String MESSAGE_INVALID_INDEX = "The index is outside the acceptable range!";

    private final Index index;
    private final String  preference;

    /**
     * @param index of the person in the address book to add preference
     * @param preference description of the preference to be added
     */
    public SavePreferenceCommand(Index index, String preference) {
        requireAllNonNull(index, preference);

        this.index = index;
        this.preference = preference;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);

        List<Person> lastShownList = model.getFilteredPersonList();

        if (index.getZeroBased() >= model.getFilteredPersonList().size()) {
            throw new CommandException(MESSAGE_INVALID_INDEX);
        }

        Person personToEdit = lastShownList.get(index.getZeroBased());
        personToEdit.getPreference().addPreference(preference);

        return new CommandResult(String.format(MESSAGE_SUCCESS, preference, Messages.format(personToEdit)));
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        // instanceof handles nulls
        if (!(other instanceof SavePreferenceCommand)) {
            return false;
        }

        SavePreferenceCommand e = (SavePreferenceCommand) other;
        return index.equals(e.index)
                && preference.equals(e.preference);
    }
}