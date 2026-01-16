package seedu.smartlib.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import seedu.smartlib.commons.core.name.Name;
import seedu.smartlib.commons.exceptions.IllegalValueException;
import seedu.smartlib.model.record.DateBorrowed;

import java.util.AbstractMap;
import java.util.Map;

/**
 * Jackson-friendly version of {@link Name DateBorrowed Pair}.
 */
class JsonAdaptedNameDateBorrowedPair {

    private static final String MESSAGE_CONSTRAINTS = "NameDateBorrowedPair creation exception";
    private final String name;
    private final String dateBorrowed;

    /**
     * Constructs a {@code JsonAdaptedNameDateBorrowedPair} with the given {@code NameDateBorrowedPair}.
     */
    @JsonCreator
    public JsonAdaptedNameDateBorrowedPair(@JsonProperty("name") String name, @JsonProperty("dateBorrowed") String dateBorrowed) {
        this.name = name;
        this.dateBorrowed = dateBorrowed;
    }

    /**
     * Converts a given {@code NameDateBorrowedPair} into this class for Jackson use.
     */
    public JsonAdaptedNameDateBorrowedPair(Map.Entry<Name, DateBorrowed> entry) {
        this.name = entry.getKey().fullName;
        this.dateBorrowed = entry.getValue().value;
    }

    /**
     * Converts a given {@code NameDateBorrowedPair} into this class for Jackson use.
     */
    public JsonAdaptedNameDateBorrowedPair(Name name, DateBorrowed dateBorrowed) {
        this.name = name.fullName;
        this.dateBorrowed = dateBorrowed.value;
    }

    /**
     * Converts this Jackson-friendly adapted NameDateBorrowedPair object into the model's {@code NameDateBorrowedPair} object.
     *
     * @throws IllegalValueException if there were any data constraints violated in the adapted NameDateBorrowedPair.
     */
    public Map.Entry<Name, DateBorrowed> toModelType() throws IllegalValueException {
        if (!Name.isValidName(name)) {
            throw new IllegalValueException(MESSAGE_CONSTRAINTS);
        }
        if (!DateBorrowed.isValidDate(dateBorrowed)) {
            throw new IllegalValueException(MESSAGE_CONSTRAINTS);
        }
        return new AbstractMap.SimpleEntry<>(new Name(name), new DateBorrowed(dateBorrowed));
    }

}