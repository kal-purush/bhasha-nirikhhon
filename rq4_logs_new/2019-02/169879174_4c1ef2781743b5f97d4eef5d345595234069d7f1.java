package planmysem.commands;

import java.util.HashSet;
import java.util.Set;

import planmysem.common.Utils;
import planmysem.data.exception.IllegalValueException;
import planmysem.data.slot.Description;
import planmysem.data.slot.Location;
import planmysem.data.slot.Name;
import planmysem.data.slot.ReadOnlySlot;
import planmysem.data.slot.Slot;
import planmysem.data.tag.Tag;

/**
 * Adds a person to the address book.
 */
public class AddCommandP extends CommandP {

    public static final String COMMAND_WORD = "add";

    public static final String MESSAGE_USAGE = COMMAND_WORD + ":\n" + "Adds a slot to the Planner. "
            + "Contact details can be marked private by prepending 'p' to the prefix.\n\t"
            + "Parameters: NAME [p]p/PHONE [p]e/EMAIL [p]a/ADDRESS  [t/TAG]...\n\t"
            + "Example: " + COMMAND_WORD
            + " John Doe p/98765432 e/johnd@gmail.com a/311, Clementi Ave 2, #02-25 t/friends t/owesMoney";

    public static final String MESSAGE_SUCCESS = "New slot added: %1$s";
    // public static final String MESSAGE_DUPLICATE_PERSON = "This person already exists in the address book";

    private final Slot slot;

    /**
     * Convenience constructor using raw values.
     *
     * @throws IllegalValueException if any of the raw values are invalid
     */
    public AddCommandP(String name, String location, String description, String startTime,
                      int duration, Set<String> tags) throws IllegalValueException {
        final Set<Tag> tagSet = new HashSet<>();

        for (String tagName : tags) {
            tagSet.add(new Tag(tagName));
        }

        this.slot = new Slot(new Name(name), new Location(location), new Description(description),
                Utils.getLocalTime(startTime), duration, tagSet);
    }

    public AddCommandP(Slot slot) {
        this.slot = slot;
    }
    public ReadOnlySlot getSlot() {
        return slot;
    }

    @Override
    public CommandResultP execute() {
        // try {
        planner.addSlot(slot);
        return new CommandResultP(String.format(MESSAGE_SUCCESS, slot));
        //        } catch (UniquePersonList.DuplicatePersonException dpe) {
        //            return new CommandResult(MESSAGE_DUPLICATE_PERSON);
        //        }
    }

}