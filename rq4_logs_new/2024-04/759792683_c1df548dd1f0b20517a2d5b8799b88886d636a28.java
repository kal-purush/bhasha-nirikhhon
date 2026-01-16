package seedu.address.model.person;

import static java.util.Objects.requireNonNull;

/**
 * Represents a Student's note in the address book. The note can be empty.
 */
public class Note {
    public static final String EMPTY_NOTE_MESSAGE = "<Note is empty. Please use the edit command to modify the note>";
    public final String value;

    /**
     * Creates an undefined note status for the student.
     */
    public Note() {
        value = EMPTY_NOTE_MESSAGE;
    }

    /**
     * Constructs an {@code Note}.
     *
     * @param note A student note.
     */
    public Note(String note) {
        requireNonNull(note);
        value = note.isEmpty() ? EMPTY_NOTE_MESSAGE : note;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        // instanceof handles nulls
        if (!(other instanceof seedu.address.model.person.Note)) {
            return false;
        }

        seedu.address.model.person.Note otherNote = (seedu.address.model.person.Note) other;
        return value.equals(otherNote.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

}