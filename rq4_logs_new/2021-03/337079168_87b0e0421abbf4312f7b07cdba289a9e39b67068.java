package seedu.smartlib.model.reader;

import java.util.List;
import java.util.function.Predicate;

import seedu.smartlib.commons.util.StringUtil;
import seedu.smartlib.model.tag.Tag;

/**
 * Tests that a {@code Reader}'s {@code Tags} matches any of the keywords given.
 */
public class TagContainsKeywordsPredicate implements Predicate<Reader> {

    private final List<String> keywords;

    /**
     * Creates a TagContainsKeywordsPredicate.
     *
     * @param keywords list of keywords
     */
    public TagContainsKeywordsPredicate(List<String> keywords) {
        this.keywords = keywords;
    }

    /**
     * Tests if the reader's tags contains any of the keywords.
     *
     * @param reader the reader to be tested.
     * @return true if the reader's tags contains any of the keywords, and false otherwise.
     */
    @Override
    public boolean test(Reader reader) {
        return keywords
                .stream()
                .anyMatch(keyword -> {
                    for (Tag tag : reader.getTags()) {
                        if (StringUtil.containsWordIgnoreCase(tag.getTagName(), keyword)) {
                            return true;
                        }
                    }
                    return false;
                });
    }

    /**
     * Checks if this TagContainsKeywordsPredicate is equal to another TagContainsKeywordsPredicate.
     *
     * @param other the other TagContainsKeywordsPredicate to be compared.
     * @return true if this TagContainsKeywordsPredicate is equal to the other TagContainsKeywordsPredicate,
     *         and false otherwise.
     */
    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof TagContainsKeywordsPredicate // instanceof handles nulls
                && keywords.equals(((TagContainsKeywordsPredicate) other).keywords)); // state check
    }

}