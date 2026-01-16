package seedu.address.logic.commands;

import static java.util.Objects.requireNonNull;

import seedu.address.model.AddressBook;
import seedu.address.model.Model;
import seedu.address.model.person.Person;

/**
 * Clears the courses of all persons in the address book.
 */
public class ClearCoursesCommand extends Command {
    public static final String COMMAND_WORD = "clear-courses";
    public static final String MESSAGE_SUCCESS = "All courses have been cleared!";

    @Override
    public CommandResult execute(Model model) {
        requireNonNull(model);
        AddressBook newAddressBook = new AddressBook(model.getAddressBook());
        newAddressBook.getPersonList().forEach(x -> newAddressBook.setPerson(x,
                new Person(x.getName(), x.getPhone(), x.getEmail(), x.getAddress(), x.getTelehandle(),
                x.getTags(), null)));
        model.setAddressBook(newAddressBook);
        return new CommandResult(MESSAGE_SUCCESS);
    }
}