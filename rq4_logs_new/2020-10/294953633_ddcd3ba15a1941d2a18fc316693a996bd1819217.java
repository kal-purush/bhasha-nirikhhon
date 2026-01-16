package jimmy.mcgymmy.logic.commands;

import static jimmy.mcgymmy.logic.commands.CommandTestUtil.assertCommandFailure;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import jimmy.mcgymmy.logic.parser.CommandParserTestUtil.ParameterStub;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.model.ModelManager;

class ImportCommandTest {
    @Test
    public void invalidFileName() {
        ParameterStub<Path> fileParameterStub = new ParameterStub<Path>(
                "",
                Path.of("INVALID_FILE_NAME")
        );
        ImportCommand importCommand = new ImportCommand();
        importCommand.setParameters(fileParameterStub);
        ModelManager model = new ModelManager();
        assertCommandFailure(importCommand, model, ImportCommand.MESSAGE_IMPORT_FOOD_FAILURE);
    }
}