package seedu.address.model.person.sorter;

import seedu.address.model.person.Person;

/**
 * A sorter for persons in the address book based on the number of courses taken.
 * Persons are sorted from the least to the most number of courses taken.
 * Subclass of {@link PersonSorter}.
 */
public class PersonCourseSizeAscendingSorter extends PersonSorter {

    @Override
    public int compare(Person person1, Person person2) {

        int courseSizeComparison = Integer.compare(person1.getCourses().size(), person2.getCourses().size());

        if (courseSizeComparison != 0) {
            return courseSizeComparison;
        } else {
            // Sort by name when course sizes are similar.
            // Convert to uppercase to compare ASCII values
            String person1UpperCaseName = person1.getName().fullName.toUpperCase();
            String person2UpperCaseName = person2.getName().fullName.toUpperCase();

            return person1UpperCaseName.compareTo(person2UpperCaseName);
        }
    }
}