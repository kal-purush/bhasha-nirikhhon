package seedu.storemando.model.item;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import seedu.storemando.testutil.ItemBuilder;

public class LocationContainsPredicateTest {

    @Test
    public void equals() {
        String room = "room";
        String kitchen = "kitchen";

        LocationContainsPredicate firstPredicate = new LocationContainsPredicate(room);
        LocationContainsPredicate secondPredicate = new LocationContainsPredicate(kitchen);

        // same object -> returns true
        assertTrue(firstPredicate.equals(firstPredicate));

        // same values -> returns true
        LocationContainsPredicate firstPredicateCopy = new LocationContainsPredicate(room);
        assertTrue(firstPredicate.equals(firstPredicateCopy));

        // different types -> returns false
        assertFalse(firstPredicate.equals(1));

        // null -> returns false
        assertFalse(firstPredicate.equals(null));

        // different item -> returns false
        assertFalse(firstPredicate.equals(secondPredicate));
    }

    @Test
    public void test_locationContainsKeyword() {
        // Keyword exactly the same
        LocationContainsPredicate predicate = new LocationContainsPredicate("Room");
        assertTrue(predicate.test(new ItemBuilder().withLocation("Room").build()));

        // Keyword has extra alphabet
        predicate = new LocationContainsPredicate("Bedroom");
        assertFalse(predicate.test(new ItemBuilder().withLocation("Room").build()));

        // Keyword is the substring
        predicate = new LocationContainsPredicate("Bedroom");
        assertFalse(predicate.test(new ItemBuilder().withLocation("Master Bedroom").build()));
    }
}