package jimmy.mcgymmy.logic.commands;

import static jimmy.mcgymmy.logic.commands.CommandTestUtil.assertCommandFailure;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import jimmy.mcgymmy.logic.parser.CommandParserTestUtil.ParameterStub;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.model.ModelManager;

class ExportCommandTest {

    @Test
    public void directoryNotFoundError() {
        ExportCommand exportCommand = new ExportCommand();
        ParameterStub<Path> parameterStub = new ParameterStub<Path>("", Path.of("c:\\Invalid String"));
        exportCommand.setPathParameter(parameterStub);
        Model model = new ModelManager();
        assertCommandFailure(exportCommand, model, String.format(ExportCommand.MESSAGE_FAILURE, "c:\\Invalid String"));
    }

}