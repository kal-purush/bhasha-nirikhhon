package seedu.duke.model.item;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModuleTest {

    @Test
    void checkValidMcs_0mcs_returnsTrue() {
        boolean result = Module.checkValidMcs(0);
        assertTrue(result);
    }

    @Test
    void checkValidMcs_negativeMcs_returnsFalse() {
        boolean result = Module.checkValidMcs(-1);
        assertFalse(result);
    }

    @Test
    void checkValidMcs_40mcs_returnsTrue() {
        boolean result = Module.checkValidMcs(40);
        assertTrue(result);
    }

    @Test
    void checkValidMcs_41mcs_returnsFalse() {
        boolean result = Module.checkValidMcs(41);
        assertFalse(result);
    }

    @Test
    void checkValidMcs_4mcs_returnsTrue() {
        boolean result = Module.checkValidMcs(4);
        assertTrue(result);
    }
}