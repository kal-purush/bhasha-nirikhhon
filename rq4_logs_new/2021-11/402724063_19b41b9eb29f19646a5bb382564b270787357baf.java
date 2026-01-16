package seedu.academydirectory.model.student;

import static java.util.Objects.requireNonNull;
import static seedu.academydirectory.logic.parser.CliSyntax.PREFIX_EMAIL;
import static seedu.academydirectory.logic.parser.CliSyntax.PREFIX_PHONE;
import static seedu.academydirectory.logic.parser.CliSyntax.PREFIX_TELEGRAM;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import seedu.academydirectory.logic.parser.Prefix;

/**
 * Obtains desired PersonalDetail among a list of Students
 */
public class PersonalDetailRetriever implements Function<Student, Optional<PersonalDetail>> {
    public static final List<Prefix> SUPPORTED_PREFIX = List.of(PREFIX_PHONE, PREFIX_EMAIL, PREFIX_TELEGRAM);

    private final Prefix prefix;
    private final NameContainsKeywordsPredicate nameContainsKeywordsPredicate;

    /**
     * Returns a Function that can be used to obtain the desired {@code PersonalDetail} among a list of Students.
     * Will return {@code PersonalDetail} tied to the given {@code Prefix} for a given {@code Name} if a Student with
     * the same name is present, or for all students if no {@code Name} is given.
     * @param prefix Prefix of the PersonalDetail that is desired
     * @param predicate Predicate used to check for Name of student whose PersonalDetail is desired.
     *                  Can be null to indicate retrieving from all Students.
     */
    public PersonalDetailRetriever(Prefix prefix, NameContainsKeywordsPredicate predicate) {
        requireNonNull(prefix);
        this.prefix = prefix;
        this.nameContainsKeywordsPredicate = predicate;
    }

    public PersonalDetailRetriever(Prefix prefix) {
        this(prefix, null);
    }

    @Override
    public Optional<PersonalDetail> apply(Student student) {
        if (nameContainsKeywordsPredicate != null && !nameContainsKeywordsPredicate.test(student)) {
            return Optional.empty();
        }

        if (PREFIX_EMAIL.equals(prefix)) {
            return Optional.of(student.getEmail());
        } else if (PREFIX_TELEGRAM.equals(prefix)) {
            return Optional.of(student.getTelegram());
        } else if (PREFIX_PHONE.equals(prefix)) {
            return student.getPhone().value.isEmpty() ? Optional.empty() : Optional.of(student.getPhone());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof PersonalDetailRetriever // instanceof handles nulls
                && prefix.equals(((PersonalDetailRetriever) other).prefix)); // state check
    }
}