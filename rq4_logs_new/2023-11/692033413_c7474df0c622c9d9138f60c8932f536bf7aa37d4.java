package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;
import static seedu.address.logic.parser.CliSyntax.PREFIX_ATTENDANCE;
import static seedu.address.model.Model.PREDICATE_SHOW_ALL_PERSONS;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import seedu.address.commons.core.index.Index;
import seedu.address.logic.Messages;
import seedu.address.logic.commands.exceptions.CommandException;
import seedu.address.model.Model;
import seedu.address.model.module.Module;
import seedu.address.model.person.Email;
import seedu.address.model.person.Name;
import seedu.address.model.person.Person;
import seedu.address.model.person.Phone;
import seedu.address.model.person.StudentNumber;
import seedu.address.model.person.Telegram;
import seedu.address.model.tag.Tag;
import seedu.address.model.tutorial.Tutorial;

/**
 * Deletes attendance from the person.
 */
public class DeleteAttendanceCommand extends Command {
    public static final String COMMAND_WORD = "deleteAttn";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ": Removes attendance for the indicated person. "
            + "Parameters: INDEX (must be a positive integer) "
            + PREFIX_ATTENDANCE + "LESSON_NUMBER\n"
            + "Example: " + COMMAND_WORD + " 2 ln/S1";

    public static final String MESSAGE_SUCCESS = "Attendance successfully deleted.";
    public static final String MESSAGE_NO_LESSON_FOUND = "The lesson number does not exist!";

    private final Index index;
    private final Tag toDelete;

    /**
     * @param index of the person in the filtered person list to add tag to
     */
    public DeleteAttendanceCommand(Index index, Tag toDelete) {
        requireNonNull(index);
        this.index = index;
        this.toDelete = toDelete;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);
        List<Person> lastShownList = model.getFilteredPersonList();
        if (index.getZeroBased() >= lastShownList.size()) {
            throw new CommandException(Messages.MESSAGE_INVALID_PERSON_DISPLAYED_INDEX);
        }

        Person personToEdit = lastShownList.get(index.getZeroBased());
        if (!personToEdit.getTags().contains(toDelete)) {
            throw new CommandException(MESSAGE_NO_LESSON_FOUND);
        }
        Person editedPerson = createEditedPerson(personToEdit);

        model.setPerson(personToEdit, editedPerson);
        model.updateFilteredPersonList(PREDICATE_SHOW_ALL_PERSONS);
        return new CommandResult(String.format(MESSAGE_SUCCESS, Messages.format(editedPerson)));
    }

    /**
     * Creates and returns a {@code Person} with the details of {@code personToEdit}
     * edited with {@code editPersonDescriptor}.
     */
    private Person createEditedPerson(Person personToEdit) {
        assert personToEdit != null;

        Name updatedName = personToEdit.getName();
        Phone updatedPhone = personToEdit.getPhone();
        Email updatedEmail = personToEdit.getEmail();

        Set<Tag> updatedTags = new HashSet<>();
        for (Tag tag : personToEdit.getTags()) {
            if (!tag.equals(toDelete)) {
                updatedTags.add(tag);
            }
        }

        Set<Module> updatedModules = new HashSet<>(personToEdit.getModules());
        Set<Tutorial> updatedTutorials = personToEdit.getTutorials();
        StudentNumber updatedStudentNumber = personToEdit.getStudentNumber();
        Telegram updatedTelegram = personToEdit.getTelegram();

        return new Person(updatedName, updatedPhone, updatedEmail, updatedTags, updatedModules,
                updatedTutorials, updatedStudentNumber, updatedTelegram);

    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof DeleteAttendanceCommand)) {
            return false;
        }

        return this.index.equals(((DeleteAttendanceCommand) other).index)
                && this.toDelete.equals(((DeleteAttendanceCommand) other).toDelete);
    }
}