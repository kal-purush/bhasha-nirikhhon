package seedu.address.model.profile.course.module;

/**
 * Represents a Module's list of preclusions in the module list.
 */
public class Preclusions {

    public final String preclusions;

    /**
     * Constructs a {@code Preclusions}.
     *
     * @param preclusions A valid string of preclusions.
     */
    public Preclusions(String preclusions) {
        this.preclusions = preclusions;
    }

    @Override
    public String toString() {
        return preclusions;
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof Preclusions // instanceof handles nulls
                && preclusions.equals(((Preclusions) other).preclusions)); // state check
    }
}