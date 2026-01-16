package seedu.address.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import seedu.address.commons.exceptions.IllegalValueException;
import seedu.address.model.project.Member;
import seedu.address.model.project.Task;
import seedu.address.model.tag.Tag;

public class JsonAdaptedTask {
    private final String name;

    /**
     * Constructs a {@code JsonAdaptedMember} with the given {@code name}.
     */
    @JsonCreator
    public JsonAdaptedTask(String name) {
        this.name = name;
    }

    /**
     * Converts a given {@code Member} into this class for Jackson use.
     */
    public JsonAdaptedTask(Task source) {
        name = source.getName().fullName;
    }

    @JsonValue
    public String getTaskName() {
        return name;
    }

    /**
     * Converts this Jackson-friendly adapted Member object into the model's {@code Member} object.
     *
     * @throws IllegalValueException if there were any data constraints violated in the adapted Member.
     */
    public Task toModelType() throws IllegalValueException {

        return new Task(name);
    }
}