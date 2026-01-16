package seedu.address.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import seedu.address.commons.exceptions.IllegalValueException;
import seedu.address.model.project.Comment;
import seedu.address.model.project.Member;

public class JsonAdaptedComment {
    private final String authorName;
    private final String comment;


    /**
     * Constructs a {@code JsonAdaptedComment} with the given comment details.
     */
    @JsonCreator
    public JsonAdaptedComment(@JsonProperty("authorName") String authorName,
                              @JsonProperty("comment") String comment) {

        this.comment = comment;
        this.authorName = authorName;
    }

    /**
     * Converts a given {@code Member} into this class for Jackson use.
     */
    public JsonAdaptedComment(Comment comment) {
        this.comment = comment.toString();
        this.authorName = comment.getMember().toString();
    }





    /**
     * Converts this Jackson-friendly adapted Member object into the model's {@code Member} object.
     *
     * @throws IllegalValueException if there were any data constraints violated in the adapted Member.
     */
    public Comment toModelType() throws IllegalValueException {
        Member author = new Member(authorName);
        return new Comment(comment, author);
    }
}