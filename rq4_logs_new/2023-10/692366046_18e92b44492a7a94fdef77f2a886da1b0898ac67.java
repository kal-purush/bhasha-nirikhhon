package seedu.address.model.course.changes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.address.testutil.Assert.assertThrows;

import org.junit.jupiter.api.Test;

import seedu.address.model.course.Course;

public class CourseAdditionTest {
    @Test
    public void constructor_null_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new CourseAddition(null));
    }

    @Test
    public void constructor_invalidDescription_throwsIllegalArgumentException() {
        String invalidDescription = "";
        assertThrows(IllegalArgumentException.class, () -> new CourseAddition(invalidDescription));
    }

    @Test
    public void isValidCourseAddition() {
        //valid
        assertTrue(CourseAddition.isValidCourseAddition("add-CS2103T"));

        // and spelt incorrectly
        assertFalse(CourseAddition.isValidCourseAddition("ard-CS2103T"));

        // missing course
        assertFalse(CourseAddition.isValidCourseAddition("and-"));
    }

    @Test
    public void test_getCourseToAdd() {
        CourseAddition courseAddition = new CourseAddition("add-CS2103T");
        assertEquals(new Course("CS2103T"), courseAddition.getCourseToAdd());
    }

    @Test
    public void equals() {
        CourseAddition courseAddition = new CourseAddition("add-CS2103T");

        // same values -> returns true
        assertTrue(courseAddition.equals(new CourseAddition("add-CS2103T")));

        // same object -> returns true
        assertTrue(courseAddition.equals(courseAddition));

        // null -> returns false
        assertFalse(courseAddition.equals(null));

        // different types -> returns false
        assertFalse(courseAddition.equals(5.0f));

        // different values -> returns false
        assertFalse(courseAddition.equals(new CourseAddition("add-MA2001")));
    }
}