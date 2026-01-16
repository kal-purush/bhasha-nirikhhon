package jimmy.mcgymmy.logic.predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import jimmy.mcgymmy.logic.parser.exceptions.ParseException;
import jimmy.mcgymmy.model.food.Name;
import jimmy.mcgymmy.testutil.FoodBuilder;

public class DatePredicateTest {

    @Test
    public void equals() throws ParseException {

        String firstPredicateDate = "24-10-2020";
        String secondPredicateDate = "25-10-2020";

        DatePredicate firstPredicate = new DatePredicate(firstPredicateDate);
        DatePredicate secondPredicate = new DatePredicate(secondPredicateDate);

        // same object -> returns true
        assertTrue(firstPredicate.equals(firstPredicate));

        // same values -> returns true
        DatePredicate firstPredicateCopy = new DatePredicate(firstPredicateDate);
        assertTrue(firstPredicate.equals(firstPredicateCopy));

        // different types -> returns false
        assertFalse(firstPredicate.equals(1));

        // null -> returns false
        assertFalse(firstPredicate.equals(null));

        // different date -> returns false
        assertFalse(firstPredicate.equals(secondPredicate));
    }

    @Test
    public void test_datesEqualMultipleFormats_returnsTrue() throws ParseException {

        // Same format
        DatePredicate predicate = new DatePredicate("24-10-2020");
        assertTrue(predicate.test(new FoodBuilder().withName(new Name("Apple")).withDate("24-10-2020").build()));

        // Different format
        predicate = new DatePredicate("2020-10-24");
        assertTrue(predicate.test(new FoodBuilder().withName(new Name("Apple")).withDate("24-10-2020").build()));
    }

    @Test
    public void test_datesNotEqual_returnsFalse() throws ParseException {

        DatePredicate predicate = new DatePredicate("24-10-2020");
        assertFalse(predicate.test(new FoodBuilder().withName(new Name("Apple")).withDate("24-10-2019").build()));
    }

}