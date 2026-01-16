package seedu.address.model.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Set;

import org.junit.jupiter.api.Test;

import seedu.address.model.ReadOnlyAddressBook;
import seedu.address.model.course.Course;
import seedu.address.model.person.Person;
import seedu.address.model.tag.Tag;

public class SampleDataUtilTest {

    @Test
    public void testGetSamplePersons() {
        Person[] samplePersons = SampleDataUtil.getSamplePersons();
        assertNotNull(samplePersons, "Sample persons array should not be null");
        assertEquals(6, samplePersons.length, "Sample persons array size should be 6");
    }

    @Test
    public void testGetSampleAddressBook() {
        ReadOnlyAddressBook sampleAddressBook = SampleDataUtil.getSampleAddressBook();
        assertNotNull(sampleAddressBook, "Sample address book should not be null");
        assertEquals(6, sampleAddressBook.getPersonList().size(), "Sample address book should contain 6 persons");
    }

    @Test
    public void testGetTagSet() {
        Set<Tag> tagSet = SampleDataUtil.getTagSet("friend", "cf", "EMERGENCY");
        assertNotNull(tagSet, "Tag set should not be null");
        assertEquals(3, tagSet.size(), "Tag set should contain 3 tags");
    }

    @Test
    public void testGetCourseSet() {
        Set<Course> courseSet = SampleDataUtil.getCourseSet("CS2103T", "MA1521");
        assertNotNull(courseSet, "Course set should not be null");
        assertEquals(2, courseSet.size(), "Course set should contain 2 courses");
    }
}