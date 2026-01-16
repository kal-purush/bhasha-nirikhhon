package jimmy.mcgymmy.model.food;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import jimmy.mcgymmy.testutil.FoodBuilder;

class TagContainsKeywordsPredicateTest {
    @Test
    public void equals() {
        List<String> firstPredicateKeywordList = Collections.singletonList("first");
        List<String> secondPredicateKeywordList = Arrays.asList("first", "second");

        TagContainsKeywordsPredicate firstPredicate = new TagContainsKeywordsPredicate(firstPredicateKeywordList);
        TagContainsKeywordsPredicate secondPredicate = new TagContainsKeywordsPredicate(secondPredicateKeywordList);

        // same object -> returns true
        assertTrue(firstPredicate.equals(firstPredicate));

        // same values -> returns true
        TagContainsKeywordsPredicate firstPredicateCopy = new TagContainsKeywordsPredicate(firstPredicateKeywordList);
        assertTrue(firstPredicate.equals(firstPredicateCopy));

        // different types -> returns false
        assertFalse(firstPredicate.equals(1));

        // null -> returns false
        assertFalse(firstPredicate.equals(null));

        // different food -> returns false
        assertFalse(firstPredicate.equals(secondPredicate));
    }

    @Test
    public void test_foodContainsTagSameAsKeywords_returnsTrue() {
        // One keyword
        TagContainsKeywordsPredicate predicate = new TagContainsKeywordsPredicate(Collections.singletonList("Lunch"));
        assertTrue(predicate.test(new FoodBuilder().withTags("Lunch").build()));
        assertTrue(predicate.test(new FoodBuilder().withTags("Dinner", "Lunch").build()));

        // Multiple keywords
        predicate = new TagContainsKeywordsPredicate(Arrays.asList("Lunch", "Dinner"));
        assertTrue(predicate.test(new FoodBuilder().withTags("Lunch").build()));
        assertTrue(predicate.test(new FoodBuilder().withTags("Dinner").build()));
        assertTrue(predicate.test(new FoodBuilder().withTags("Dinner", "Lunch").build()));
        assertTrue(predicate.test(new FoodBuilder().withTags("Dinner", "Breakfast", "Lunch").build()));
    }

    @Test
    public void test_foodDoesNotContainsTagSameAsKeywords_returnsFalse() {
        TagContainsKeywordsPredicate predicate = new TagContainsKeywordsPredicate(Collections.singletonList("Lunch"));
        // food contains one tag
        assertFalse(predicate.test(new FoodBuilder().withTags("Dinner").build()));
        // food contains multiple tags
        assertFalse(predicate.test(new FoodBuilder().withTags("Breakfast", "Dinner").build()));
    }

    @Test
    public void test_cannotCreateTagWithKeyword_returnFalse() {
        TagContainsKeywordsPredicate predicate = new TagContainsKeywordsPredicate(Collections.singletonList(""));
        assertFalse(predicate.test(new FoodBuilder().withTags("Dinner").build()));

        predicate = new TagContainsKeywordsPredicate(Collections.singletonList("A b c"));
        assertFalse(predicate.test(new FoodBuilder().withTags("Dinner").build()));
    }

    @Test
    public void test_cannotCreateTagWithSomeKeywords() {
        TagContainsKeywordsPredicate predicate =
                new TagContainsKeywordsPredicate(Arrays.asList("AB C", "Lunch", "", "Breakfast"));
        // food contains some of the valid tag -> returns false
        assertTrue(predicate.test(new FoodBuilder().withTags("Dinner", "Lunch").build()));
        // food contains none of the valid tag -> returns false
        assertFalse(predicate.test(new FoodBuilder().withTags("Dinner").build()));
    }
}