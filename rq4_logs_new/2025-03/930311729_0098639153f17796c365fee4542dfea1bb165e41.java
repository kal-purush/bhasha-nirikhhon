package seedu.address.logic.commands;

import seedu.address.commons.core.index.Index;
import seedu.address.logic.Messages;
import seedu.address.logic.commands.exceptions.CommandException;
import seedu.address.model.Model;
import seedu.address.model.person.Person;
import seedu.address.model.tag.Tag;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static seedu.address.commons.util.CollectionUtil.requireAllNonNull;
import static seedu.address.model.Model.PREDICATE_SHOW_ALL_PERSONS;

/**
 * Tags a customer in the address book.
 */
public class TagCommand extends Command{
    public static final String COMMAND_WORD = "tag";

    public static final String MESSAGE_USAGE = COMMAND_WORD
            + ": Tags the customer identified "
            + "by the index number used in the last person listing. "
            + "Existing tag will be overwritten by the input.\n"
            + "Parameters: INDEX (must be a positive integer) "
            + "t/ [TAG (Allowed values: VIP, Regular, New)]\n"
            + "Example: " + COMMAND_WORD + " 1 "
            + "t/ VIP";

    public static final String MESSAGE_SUCCESS = "Customer %s tagged as %s";
    public static final String MESSAGE_INVALID_TAG = "Invalid tag. Allowed values: VIP, Regular, New.";
    public static final String MESSAGE_INVALID_INDEX = "The index is outside the acceptable range!";

    private final Index index;
    private final String tag;

    public TagCommand(Index index, String tag) {
        requireAllNonNull(index, tag);

        this.index = index;
        this.tag = tag;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);

        List<Person> lastShownList = model.getFilteredPersonList();

        if (index.getZeroBased() >= model.getFilteredPersonList().size()) {
            throw new CommandException(MESSAGE_INVALID_INDEX);
        }

        Tag newTag = new Tag(tag);

        Person personToEdit = lastShownList.get(index.getZeroBased());
        Person editedPerson = new Person(
                personToEdit.getName(), personToEdit.getPhone(), Optional.of(newTag));

        model.setPerson(personToEdit, editedPerson);
        model.updateFilteredPersonList(PREDICATE_SHOW_ALL_PERSONS);

        return new CommandResult(String.format(MESSAGE_SUCCESS, Messages.format(editedPerson), tag));
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        // instanceof handles nulls
        if (!(other instanceof TagCommand)) {
            return false;
        }

        TagCommand e = (TagCommand) other;
        return index.equals(e.index)
                && tag.equals(e.tag);
    }
}