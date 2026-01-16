package seedu.smartlib.model.record;

/**
 * Represents the cost of overdue books.
 */
public class Cost {
    public static final double RATES_PER_HOUR = 0.1;
    private final int overdueHour;

    /**
     * Creates a cost of overdue book.
     * @param overdueHour The duration, in hours, that the book was overdue.
     */
    public Cost(int overdueHour) {
        this.overdueHour = overdueHour;
    }

    /**
     * Calculate the total cost given the overdue hours.
     * @return The total cost of the overdue book.
     */
    public double getCost() {
        return RATES_PER_HOUR * overdueHour;
    }

    @Override
    public String toString() {
        return String.format("The total cost is %d.", this.getCost());
    }

    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof Cost // instanceof handles nulls
                && overdueHour == ((Cost) other).overdueHour); // state check
    }
}