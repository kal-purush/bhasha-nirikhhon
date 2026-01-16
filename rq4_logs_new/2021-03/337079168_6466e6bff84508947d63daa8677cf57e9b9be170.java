package seedu.smartlib.model.book;

import java.util.List;
import java.util.function.Predicate;

import seedu.smartlib.commons.util.StringUtil;

/**
 * Tests that a {@code Book}'s {@code Name} matches any of the keywords given.
 */
public class BookNameContainsKeywordsPredicate implements Predicate<Book> {
    private final List<String> keywords;

    /**
     * Creates a NameContainsKeywordPredicate
     * @param keywords list of keywords
     */
    public BookNameContainsKeywordsPredicate(List<String> keywords) {
        this.keywords = keywords;
    }

    @Override
    public boolean test(Book book) {
        return keywords.stream()
                .anyMatch(keyword -> StringUtil.containsWordIgnoreCase(book.getName().fullName, keyword));
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof BookNameContainsKeywordsPredicate // instanceof handles nulls
                && keywords.equals(((BookNameContainsKeywordsPredicate) other).keywords)); // state check
    }

}
