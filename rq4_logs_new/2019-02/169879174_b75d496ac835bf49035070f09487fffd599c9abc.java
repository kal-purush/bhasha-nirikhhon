package planmysem.data.slot;

import planmysem.data.exception.IllegalValueException;

/**
 * Represents a Person's email in the address book.
 * Guarantees: immutable; is valid as declared in {@link #isValid(String)}
 */
public class DateTime {

    public static final String EXAMPLE = "valid@e.mail";
    public static final String MESSAGE_EMAIL_CONSTRAINTS =
            "Person emails should be 2 alphanumeric/period strings separated by '@'";
    public static final String EMAIL_VALIDATION_REGEX = "[\\w\\.]+@[\\w\\.]+";

    public final String value;
    private boolean isPrivate;

    /**
     * Validates given email.
     *
     * @throws IllegalValueException if given email address string is invalid.
     */
    public DateTime(String email, boolean isPrivate) throws IllegalValueException {
        this.isPrivate = isPrivate;
        email = email.trim();
        if (!isValid(email)) {
            throw new IllegalValueException(MESSAGE_EMAIL_CONSTRAINTS);
        }
        this.value = email;
    }

    /**
     * Checks if a given string is a valid Date and Time.
     */
    public static boolean isValid(String test) {
        return test.matches(EMAIL_VALIDATION_REGEX);
    }

    public boolean isPrivate() {
        return isPrivate;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}