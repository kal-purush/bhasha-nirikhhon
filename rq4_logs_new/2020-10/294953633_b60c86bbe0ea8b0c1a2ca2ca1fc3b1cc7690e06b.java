package jimmy.mcgymmy.logic.commands;

import static jimmy.mcgymmy.testutil.TypicalMacros.TEST_MACRO;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jimmy.mcgymmy.logic.commands.exceptions.CommandException;
import jimmy.mcgymmy.logic.parser.CommandParserTestUtil;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.model.ModelManager;
import jimmy.mcgymmy.model.UserPrefs;
import jimmy.mcgymmy.testutil.TypicalFoods;

public class ListMacroCommandTest {
    private Model model;

    @BeforeEach
    public void setUp() throws Exception {
        this.model = new ModelManager(TypicalFoods.getTypicalMcGymmy(), new UserPrefs());
        this.model.setMacroList(this.model.getMacroList().withNewMacro(TEST_MACRO));
    }

    @Test
    public void listWithoutParameter_success() throws Exception {
        String feedback = new ListMacroCommand().execute(model).getFeedbackToUser();

        // testing if its showing whole list
        assertTrue(feedback.contains("all the available macros"));
        assertTrue(feedback.contains("test"));
    }

    @Test
    public void listWithParameter_validName_success() throws Exception {
        ListMacroCommand listMacroCommand = new ListMacroCommand();
        listMacroCommand.setParameters(new CommandParserTestUtil.OptionalParameterStub<>("", "test"));
        String feedback = listMacroCommand.execute(model).getFeedbackToUser();

        // testing if its showing specific macro
        assertTrue(feedback.contains("Parameters:"));
        assertTrue(feedback.contains("Information on macro test:"));
    }

    @Test
    public void listWithParameter_invalidName_throwsCommandException() throws Exception {
        ListMacroCommand listMacroCommand = new ListMacroCommand();
        listMacroCommand.setParameters(new CommandParserTestUtil.OptionalParameterStub<>("", "wuyweg"));
        assertThrows(CommandException.class, () -> listMacroCommand.execute(model));
    }
}