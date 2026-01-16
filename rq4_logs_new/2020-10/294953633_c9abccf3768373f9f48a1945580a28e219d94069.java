package jimmy.mcgymmy.storage;

import static jimmy.mcgymmy.testutil.Assert.assertThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import jimmy.mcgymmy.commons.exceptions.IllegalValueException;
import jimmy.mcgymmy.logic.macro.Macro;

public class JsonAdaptedMacroTest {
    @Test
    public void validMacro_success() throws Exception {
        String name = "test";
        String[] macroArguments = new String[]{"a","b"};
        String[] rawCommands = new String[]{"add -n \\a -f \\b"};
        JsonAdaptedMacro jsonAdaptedMacro = new JsonAdaptedMacro(name, macroArguments, rawCommands);
        Macro macro = new Macro(name, macroArguments, rawCommands);
        Macro macroFromJson = jsonAdaptedMacro.toMacro();
        Assertions.assertEquals(macro.getName(), macroFromJson.getName());
        Assertions.assertEquals(macro.getMacroArguments(), macroFromJson.getMacroArguments());
        Assertions.assertEquals(macro.getRawCommands(), macroFromJson.getRawCommands());
    }

    @Test
    public void invalidMacro_throwsIllegalValueException() throws Exception {
        String name = "test";
        String[] macroArguments = "-a -b".split(" ");
        String[] rawCommands = "add -n \\a -f \\b".split(";");
        JsonAdaptedMacro jsonAdaptedMacro = new JsonAdaptedMacro(name, macroArguments, rawCommands);
        assertThrows(IllegalValueException.class, jsonAdaptedMacro::toMacro);
    }
}