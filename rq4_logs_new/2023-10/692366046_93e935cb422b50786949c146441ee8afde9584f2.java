package seedu.address.model.person.sorter;

import static seedu.address.commons.util.CollectionUtil.requireAllNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import seedu.address.model.person.Person;
import seedu.address.model.tag.Tag;

/**
 * A sorter for persons in the address book based on the tags.
 * Subclass of {@link PersonSorter}.
 */
public class PersonTagSorter extends PersonSorter {

    /**
     * Constructs a {@code PersonTagSorter}.
     */
    public PersonTagSorter() {
        criterion = "tags-friends";
    }

    @Override
    public int compare(Person person1, Person person2) {
        requireAllNonNull(person1, person2);

        int tagComparison = compareTags(person1.getTags(), person2.getTags());

        if (tagComparison != 0) {
            return tagComparison;
        } else {
            // If tags are similar, sort by name
            String person1UpperCaseName = person1.getName().fullName.toUpperCase();
            String person2UpperCaseName = person2.getName().fullName.toUpperCase();

            return person1UpperCaseName.compareTo(person2UpperCaseName);
        }
    }

    private int compareTags(Set<Tag> person1Tags, Set<Tag> person2Tags) {
        int person1TagPriority = getMaxTagPriority(person1Tags);
        int person2TagPriority = getMaxTagPriority(person2Tags);

        return Integer.compare(person1TagPriority, person2TagPriority);
    }

    private int getMaxTagPriority(Set<Tag> tags) {
        Map<String, Integer> tagPriority = new HashMap<>();
        tagPriority.put("Close Friend", 1);
        tagPriority.put("Friend", 2);
        tagPriority.put("Emergency", 3);

        int maxPriority = Integer.MAX_VALUE;

        for (Tag tag : tags) {
            String tagName = tag.getTagName();
            if (tagPriority.containsKey(tagName)) {
                maxPriority = Math.min(maxPriority, tagPriority.get(tagName));
            }
        }

        return maxPriority;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof PersonTagSorter)) {
            return false;
        }

        PersonTagSorter otherPersonTagSorter = (PersonTagSorter) other;
        return this.getSortingCriterion().equals(otherPersonTagSorter.getSortingCriterion());
    }

    @Override
    public int hashCode() {
        return criterion.hashCode();
    }
}