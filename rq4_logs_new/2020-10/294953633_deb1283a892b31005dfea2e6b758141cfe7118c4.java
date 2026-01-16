package jimmy.mcgymmy.model.macro;

import static jimmy.mcgymmy.testutil.TypicalMacros.TEST_MACRO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;


public class MacroTest {
    @Test
    void testMacro_createdWithCorrectOptions() {
        assertTrue(TEST_MACRO.getOptions().hasOption("c"));
        assertTrue(TEST_MACRO.getOptions().hasOption("p"));
    }

    @Test
    void testMacro_noExtraOptions() {
        assertEquals(TEST_MACRO.getOptions().getOptions().size(), 2);
    }
}