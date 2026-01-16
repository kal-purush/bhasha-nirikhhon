package jimmy.mcgymmy.logic.macro;

import static jimmy.mcgymmy.testutil.Assert.assertThrows;

import java.util.Set;

import org.junit.jupiter.api.Test;

import jimmy.mcgymmy.logic.macro.exceptions.DuplicateMacroException;

public class MacroListTest {
    @Test
    public void addDuplicate_throwsError() throws Exception {
        MacroList macroList = new MacroList(Set.of("test1"));
        Macro dummyMacro = new Macro("test2", new String[]{}, new String[]{});
        Macro duplicateMacro1 = new Macro("test1", new String[]{}, new String[]{});
        Macro duplicateMacro2 = new Macro("test2", new String[]{}, new String[]{});
        macroList.addMacro(dummyMacro);
        assertThrows(DuplicateMacroException.class, ()->macroList.addMacro(duplicateMacro1));
        assertThrows(DuplicateMacroException.class, ()->macroList.addMacro(duplicateMacro2));
    }

    // TODO test serialization?
}