package seedu.academydirectory.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static seedu.academydirectory.logic.parser.CliSyntax.PREFIX_EMAIL;
import static seedu.academydirectory.testutil.TypicalIndexes.INDEX_FIRST_STUDENT;

import org.junit.jupiter.api.Test;

import seedu.academydirectory.logic.commands.AddCommand;
import seedu.academydirectory.logic.commands.Command;
import seedu.academydirectory.logic.commands.GetCommand;
import seedu.academydirectory.logic.commands.HelpCommand;
import seedu.academydirectory.logic.commands.HistoryCommand;
import seedu.academydirectory.logic.commands.ListCommand;
import seedu.academydirectory.logic.commands.RedoCommand;
import seedu.academydirectory.logic.commands.ShowCommand;
import seedu.academydirectory.logic.commands.ViewCommand;
import seedu.academydirectory.logic.commands.VisualizeCommand;
import seedu.academydirectory.model.student.InformationWantedFunction;

public class AdditionalViewTypeTest {
    @Test
    public void parse_additionalViewType() {
        Command viewCommand = new ViewCommand(INDEX_FIRST_STUDENT);
        Command visualizeCommand = new VisualizeCommand();
        Command historyCommand = new HistoryCommand();
        Command helpCommand = new HelpCommand();
        Command getCommand = new GetCommand(new InformationWantedFunction(PREFIX_EMAIL, null));
        Command showCommand = new ShowCommand("RA1");
        Command redoCommand = new RedoCommand();
        Command addCommand = new ListCommand();

        // Additional View Type works for View
        assertEquals(AdditionalViewType.parse(viewCommand), AdditionalViewType.VIEW);

        // Additional View Type works for Visualize
        assertEquals(AdditionalViewType.parse(visualizeCommand), AdditionalViewType.VISUALIZE);

        // Additional View Type works for History
        assertEquals(AdditionalViewType.parse(historyCommand), AdditionalViewType.TEXT);

        // Additional View Type works for Help
        assertEquals(AdditionalViewType.parse(helpCommand), AdditionalViewType.HELP);

        // Additional View Type works for Get
        assertEquals(AdditionalViewType.parse(getCommand), AdditionalViewType.TEXT);

        // Additional View Type works for Show
        assertEquals(AdditionalViewType.parse(showCommand), AdditionalViewType.TEXT);

        // Additional View Type returns default for anything else
        assertEquals(AdditionalViewType.parse(redoCommand), AdditionalViewType.DEFAULT);
        assertEquals(AdditionalViewType.parse(addCommand), AdditionalViewType.DEFAULT);
        assertNotEquals(AdditionalViewType.parse(viewCommand), AdditionalViewType.DEFAULT);
    }
}