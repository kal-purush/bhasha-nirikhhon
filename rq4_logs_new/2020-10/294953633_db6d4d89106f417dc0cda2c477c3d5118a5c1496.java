package jimmy.mcgymmy.model.date;

import static java.util.Objects.requireNonNull;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import jimmy.mcgymmy.commons.util.AppUtil;

public class Date {
    public static final String MESSAGE_CONSTRAINTS = "Date string should be in one of the following formats:\n"
            + "yyyy-M-dd\n"
            + "yyyy-M-d\n"
            + "dd-MM-yyyy\n"
            + "dd-M-yyyy\n"
            + "d-M-yyyy\n"
            + "yyyy/MM/dd\n"
            + "dd/MM/yyyy\n"
            + "dd/M/yyyy\n"
            + "d/M/yyyy\n";

    private static final String[] SUPPORTED_FORMATS = {
        "yyyy-MM-dd",
        "yyyy-M-dd",
        "yyyy-M-d",
        "dd-MM-yyyy",
        "dd-M-yyyy",
        "d-M-yyyy",
        "yyyy/MM/dd",
        "dd/MM/yyyy",
        "dd/M/yyyy",
        "d/M/yyyy"
    };

    private static final String OUTPUT_FORMAT = "d MMM yyyy";

    private LocalDate date;

    /**
     * Constructs a {@code Date}.
     *
     * @param date A valid date string.
     */
    public Date(String date) {
        requireNonNull(date);
        boolean canParse = false;
        for (String format : SUPPORTED_FORMATS) {
            try {
                this.date = LocalDate.parse(date, DateTimeFormatter.ofPattern(format));
                canParse = true;
                break;
            } catch (DateTimeParseException e) {
                // exception is thrown means that cannot parse date in that format
                // if cannot parse date in any format, throw IllegalArgumentException
            }
        }
        AppUtil.checkArgument(canParse, MESSAGE_CONSTRAINTS);
    }

    private Date() {
        date = LocalDate.now();
    }

    public static Date currentDate() {
        return new Date();
    }

    @Override
    public boolean equals(Object other) {
        return other == this
            || (other instanceof Date)
            && this.date.equals(((Date) other).date);
    }

    @Override
    public int hashCode() {
        return date.hashCode();
    }

    /**
     * Format state as text for viewing.
     */
    @Override
    public String toString() {
        return date.format(DateTimeFormatter.ofPattern(OUTPUT_FORMAT));
    }
}