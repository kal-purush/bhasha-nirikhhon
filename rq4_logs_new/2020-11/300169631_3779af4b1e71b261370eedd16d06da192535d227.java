package seedu.duke.task;

import seedu.duke.DukeException;
import seedu.duke.common.Messages;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Represents an expense item.
 */
public class Expense extends Item {

    private final double value;
    private String currency;
    private LocalDate date;
    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private LocalDate now = LocalDate.now();

    public Expense(String description, double value) {
        super(description);
        this.value = value;
        this.currency = "SGD";
        this.date = now;
    }

    @Override
    public String toString() {
        return String.format("%s (%.2d %s) (date: %s)", getDescription(), getValue(), getCurrency(), dateTimeFormatter.format(getDate()));
    }

    public String getDescription() {
        return description;
    }

    public double getValue() {
        return value;
    }

    public String getCurrency() {
        return currency;
    }

    @Override
    public LocalDate getDate() {
        return date;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setDate(String date) throws DukeException {
        try {
            this.date = LocalDate.parse(date, dateTimeFormatter);
        } catch (DateTimeParseException e) {
            throw new DukeException(Messages.EXCEPTION_WRONG_DATE_FORMAT);
        }
    }
}