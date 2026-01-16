package jimmy.mcgymmy.logic.commands;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.logging.Logger;

import jimmy.mcgymmy.logic.commands.exceptions.CommandException;
import jimmy.mcgymmy.logic.parser.ParserUtil;
import jimmy.mcgymmy.logic.parser.parameter.Parameter;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.storage.JsonMcGymmyStorage;

public class ExportCommand extends Command {

    public static final String COMMAND_WORD = "export";
    public static final String SHORT_DESCRIPTION = "Export McGymmy save file to directory";
    public static final String MESSAGE_SUCCESS = "Exported to %s";
    public static final String MESSAGE_FAILURE = "File failed to save to %s";
    public static final String DEFAULT_FILENAME = "/mcgymmy.json";

    private Parameter<Path> pathParameter = addParameter(
            "directoryPath",
            "",
            "Path to directory to save McGymmy file",
            "C:/mcgymmy",
            ParserUtil::parseDir
    );

    public void setPathParameter(Parameter<Path> pathParameter) {
        this.pathParameter = pathParameter;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        //Get the path parameter
        Path path = pathParameter.consume();

        //Check if the directory exists
        File file = new File(path.toString());
        if (!file.exists()) {
            throw new CommandException(String.format(MESSAGE_FAILURE, path.toString()));
        }

        //Add the mcgymmy filename to export
        path = Path.of(path.toString() + DEFAULT_FILENAME);
        JsonMcGymmyStorage mcGymmyStorage = new JsonMcGymmyStorage(path);
        try {
            mcGymmyStorage.saveMcGymmy(model.getMcGymmy());
        } catch (IOException e) {
            Logger.getLogger("Export Command").info(e.toString());
            throw new CommandException(String.format(MESSAGE_FAILURE, path.toString()));
        }

        //Return the successful commandResult
        return new CommandResult(String.format(MESSAGE_SUCCESS, path.toString()));
    }
}