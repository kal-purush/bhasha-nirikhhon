package seedu.address.model.module;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ModuleTest {

    @Test
    public void equals() {
        Module firstModule = new Module("CS2100");
        Module secondModule = new Module("CS2100");

        assertTrue(firstModule.equals(secondModule));
        assertFalse(firstModule.equals(2));
    }
}