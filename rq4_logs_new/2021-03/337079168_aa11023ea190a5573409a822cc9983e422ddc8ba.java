package seedu.smartlib.model.record;

import java.util.function.Predicate;

import seedu.smartlib.model.book.Barcode;

/**
 * Tests that a {@code Record}'s {@code Name} matches the barcode given.
 */
public class RecordContainsBarcodePredicate implements Predicate<Record> {

    private final Barcode barcode;

    /**
     * Creates a RecordNameContainsKeywordPredicate.
     *
     * @param barcode barcode of the book
     */
    public RecordContainsBarcodePredicate(Barcode barcode) {
        this.barcode = barcode;
    }

    /**
     * Tests if the record contains the given barcode.
     *
     * @param record the record to be tested.
     * @return true if the record contains the barcode, and false otherwise.
     */
    @Override
    public boolean test(Record record) {

        return this.barcode.equals(record.getBookBarcode());

    }

    /**
     * Checks if this RecordNameContainsKeywordPredicate is equal to another RecordNameContainsKeywordPredicate.
     *
     * @param other the other RecordNameContainsKeywordPredicate to be compared.
     * @return true if this RecordNameContainsKeywordPredicate is equal to the other RecordNameContainsKeywordPredicate,
     *         and false otherwise.
     */
    @Override
    public boolean equals(Object other) {
        return other == this // short circuit if same object
                || (other instanceof seedu.smartlib.model.record.RecordContainsBarcodePredicate // instanceof handles nulls
                && barcode.equals(((seedu.smartlib.model.record.RecordContainsBarcodePredicate) other).barcode)); // state check
    }

}
