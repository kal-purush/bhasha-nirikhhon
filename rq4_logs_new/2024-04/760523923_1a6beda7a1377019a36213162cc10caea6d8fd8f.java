package seedu.address.model.person;

import java.util.function.Predicate;

import seedu.address.commons.util.ToStringBuilder;
import seedu.address.logic.commands.FindCommand;

/**
 * Tests that a {@code Person}'s {@code NusId} matches any of the keywords given.
 */
public class NusIdMatchesPredicate implements Predicate<Person> {
    private final String keyword;

    public NusIdMatchesPredicate(String keyword) {
        this.keyword = keyword;
    }

    @Override
    public boolean test(Person person) {
        if (keyword.equals(FindCommand.NOT_REQUIRED_VALUE)) {
            return true;
        } else if (NusId.isValidFindNusId(keyword)) {
            return person.getNusId().toString().startsWith(keyword.trim());
        } else {
            return false;
        }

    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .add("nusIdKeyword", keyword)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        // instanceof handles nulls
        if (!(other instanceof NusIdMatchesPredicate)) {
            return false;
        }

        NusIdMatchesPredicate predicate = (NusIdMatchesPredicate) other;
        return keyword.equals(predicate.keyword);
    }
}