package seedu.smartlib.model.book;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static seedu.smartlib.testutil.Assert.assertThrows;

import org.junit.jupiter.api.Test;

public class BarcodeTest {

    @Test
    public void constructor_invalidBarcode_throwsAssertionError() {
        int invalidBarcode = 1;
        assertThrows(AssertionError.class, () -> new Barcode(invalidBarcode));
    }

    @Test
    public void isValidBarcode() {

        // invalid barcode numbers
        assertFalse(Barcode.isValidBarcode(1000000000-1)); // less than Barcode.MIN_VALUE
        assertFalse(Barcode.isValidBarcode(123)); // less than Barcode.MIN_VALUE
        assertFalse(Barcode.isValidBarcode(2000000000+1)); // larger than Barcode.MAX_VALUE
        assertFalse(Barcode.isValidBarcode(2000000010)); // larger than Barcode.MIN_VALUE


        // valid barcode numbers
        assertTrue(Barcode.isValidBarcode(1080060009)); // >= Barcode.MIN_VALUE && <= Barcode.MAX_VALUE
        assertTrue(Barcode.isValidBarcode(1000000000)); // exactly Barcode.MIN_VALUE
        assertTrue(Barcode.isValidBarcode(1000000001)); // Barcode.MIN_VALUE + 1
        assertTrue(Barcode.isValidBarcode(2000000000)); // exactly Barcode.MAX_VALUE
        assertTrue(Barcode.isValidBarcode(1999999999)); // Barcode.MAX_VALUE + 1
    }
}