package seedu.address.model.util;

import java.util.List;

import seedu.address.model.person.Person;
import seedu.address.model.tag.Tag;

/**
 * Utility class for handling operations related to {@code Tag} objects within the application.
 */
public class TagUtil {

    public static final String EMERGENCY_TAG_LIMIT_MESSAGE =
            "Cannot add more than two contacts with the 'emergency' tag";

    /**
     * Checks if adding a person with the 'emergency' tag will exceed the allowed limit of 2.
     *
     * @param person the person to be added or edited
     * @param currentPersons the current list of persons
     * @return true if it is safe to add or edit the person, false otherwise
     */
    public static boolean canAddOrEditEmergencyTag(Person person, List<Person> currentPersons) {
        if (person.getTags().stream().anyMatch(Tag::isEmergencyTag)) {
            long emergencyTagCount = currentPersons.stream()
                    .flatMap(p -> p.getTags().stream())
                    .filter(Tag::isEmergencyTag)
                    .count();
            return (emergencyTagCount < 2);
        }
        return true;
    }
}