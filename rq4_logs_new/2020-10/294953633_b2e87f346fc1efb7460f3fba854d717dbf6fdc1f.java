package jimmy.mcgymmy.model.food;

import java.util.List;
import java.util.function.Predicate;

/**
 * Tests that a {@code Food} matches any of the keywords given.
 */
public class FoodContainsKeywordsPredicate implements Predicate<Food> {
    private final List<String> keywords;

    public FoodContainsKeywordsPredicate(List<String> keywords) {
        this.keywords = keywords;
    }

    @Override
    public boolean test(Food food) {
        return new TagContainsKeywordsPredicate(keywords).test(food)
                || new NameContainsKeywordsPredicate(keywords).test(food);
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof FoodContainsKeywordsPredicate // instanceof handles nulls
                && keywords.equals(((FoodContainsKeywordsPredicate) other).keywords)); // state check
    }
}