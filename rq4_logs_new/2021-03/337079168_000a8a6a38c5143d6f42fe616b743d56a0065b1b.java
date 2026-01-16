package seedu.smartlib.testutil;

import seedu.smartlib.model.SmartLib;
import seedu.smartlib.model.reader.Reader;

/**
 * A utility class to help with building SmartLib objects.
 * Example usage: <br>
 *     {@code SmartLib sl = new SmartLibBuilder().withReader("John", "Doe").build();}
 */
public class SmartLibBuilder {

    private SmartLib smartLib;

    public SmartLibBuilder() {
        smartLib = new SmartLib();
    }

    public SmartLibBuilder(SmartLib smartLib) {
        this.smartLib = smartLib;
    }

    /**
     * Adds a new {@code Person} to the {@code SmartLib} that we are building.
     */
    public SmartLibBuilder withReader(Reader reader) {
        smartLib.addReader(reader);
        return this;
    }

    public SmartLib build() {
        return smartLib;
    }
}