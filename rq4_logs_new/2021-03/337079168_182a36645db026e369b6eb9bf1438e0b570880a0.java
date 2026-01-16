package seedu.smartlib.model.book;

import seedu.smartlib.commons.core.name.Name;

/**
 * Represents a genre of books in SmartLib.
 */
public class Genre {

    public static final String MESSAGE_CONSTRAINTS =
            "Genre should only contain alphanumeric characters and spaces, and it should not be blank";

    private final Name genreName;

    /**
     * Constructs an {@code Genre}.
     *
     * @param genreName A valid name of a genre.
     */
    public Genre(Name genreName) {
        this.genreName = genreName;
    }

    /**
     * Returns true if a given string is a valid genre name.
     */
    public static boolean isValidGenre(String test) {
        return Name.isValidName(test);
    }

    @Override
    public String toString() {
        return genreName.toString();
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof Genre // instanceof handles nulls
                && genreName.equals(((Genre) other).genreName)); // state check
    }

    /**
     * Calculates hashCode of this Genre object
     * @return hashCode of Genre
     */
    @Override
    public int hashCode() {
        return genreName.hashCode();
    }

}