package seedu.address.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import seedu.address.commons.exceptions.IllegalValueException;
import seedu.address.model.project.Member;
import seedu.address.model.tag.Tag;

public class JsonAdaptedMember {
    private final String name;

    /**
     * Constructs a {@code JsonAdaptedMember} with the given {@code name}.
     */
    @JsonCreator
    public JsonAdaptedMember(String name) {
        this.name = name;
    }

    /**
     * Converts a given {@code Member} into this class for Jackson use.
     */
    public JsonAdaptedMember(Member source) {
        name = source.getName().fullName;
    }

    @JsonValue
    public String getMemberName() {
        return name;
    }

    /**
     * Converts this Jackson-friendly adapted Member object into the model's {@code Member} object.
     *
     * @throws IllegalValueException if there were any data constraints violated in the adapted Member.
     */
    public Member toModelType() throws IllegalValueException {
        if (!Tag.isValidTagName(name)) {
            throw new IllegalValueException(Tag.MESSAGE_CONSTRAINTS);
        }
        return new Member(name);
    }
}