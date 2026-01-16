package pwe.planner.model.planner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static pwe.planner.testutil.TypicalDegreePlanners.YEAR_1_SEMESTER_1;
import static pwe.planner.testutil.TypicalDegreePlanners.YEAR_1_SEMESTER_2;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import pwe.planner.testutil.DegreePlannerBuilder;

public class DegreePlannerTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void asObservableList_modifyList_throwsUnsupportedOperationException() {
        DegreePlanner degreePlanner = new DegreePlannerBuilder().build();
        thrown.expect(UnsupportedOperationException.class);
        degreePlanner.getCodes().remove(0);
    }

    @Test
    public void isSameDegreePlanner() {
        // same object -> returns true
        assertTrue(YEAR_1_SEMESTER_1.isSameDegreePlanner(YEAR_1_SEMESTER_1));

        // null -> returns false
        assertFalse(YEAR_1_SEMESTER_1.isSameDegreePlanner(null));

        // different year and code -> returns false
        DegreePlanner editedDegreePlanner = new DegreePlannerBuilder(YEAR_1_SEMESTER_1)
                .withYear("2").withCodes("CS1111", "CS2222").build();
        assertFalse(YEAR_1_SEMESTER_1.isSameDegreePlanner(editedDegreePlanner));

        // different year but same code -> returns false
        editedDegreePlanner = new DegreePlannerBuilder(YEAR_1_SEMESTER_1).withYear("2").build();
        assertFalse(YEAR_1_SEMESTER_1.isSameDegreePlanner(editedDegreePlanner));

        // different year but same semester -> returns false
        editedDegreePlanner = new DegreePlannerBuilder(YEAR_1_SEMESTER_1).withYear("2").withSemester("1").build();
        assertFalse(YEAR_1_SEMESTER_1.isSameDegreePlanner(editedDegreePlanner));

        // same year, same semester, different codes -> returns true
        editedDegreePlanner = new DegreePlannerBuilder(YEAR_1_SEMESTER_1).withCodes("CS1111", "CS2222").build();
        assertTrue(YEAR_1_SEMESTER_1.isSameDegreePlanner(editedDegreePlanner));
    }

    @Test
    public void equals() {
        // same values -> returns true
        DegreePlanner degreePlannerCopy = new DegreePlannerBuilder(YEAR_1_SEMESTER_1).build();
        assertTrue(YEAR_1_SEMESTER_1.equals(degreePlannerCopy));

        // same object -> returns true
        assertTrue(YEAR_1_SEMESTER_1.equals(YEAR_1_SEMESTER_1));

        // different type -> returns false
        assertFalse(YEAR_1_SEMESTER_1.equals(5));

        // different degree plan -> returns false
        assertFalse(YEAR_1_SEMESTER_1.equals(YEAR_1_SEMESTER_2));

        // different year -> returns false
        DegreePlanner editedDegreePlanner = new DegreePlannerBuilder(YEAR_1_SEMESTER_1).withYear("2").build();
        assertFalse(YEAR_1_SEMESTER_1.equals(editedDegreePlanner));

        // different semester -> returns false
        editedDegreePlanner = new DegreePlannerBuilder(YEAR_1_SEMESTER_1).withSemester("2").build();
        assertFalse(YEAR_1_SEMESTER_1.equals(editedDegreePlanner));

        // different codes -> returns false
        editedDegreePlanner = new DegreePlannerBuilder(YEAR_1_SEMESTER_1).withCodes("CS1111", "CS2222").build();
        assertFalse(YEAR_1_SEMESTER_1.equals(editedDegreePlanner));
    }
}