package jimmy.mcgymmy.logic.commands;

import java.nio.file.Path;
import java.util.Optional;

import jimmy.mcgymmy.commons.exceptions.DataConversionException;
import jimmy.mcgymmy.logic.commands.exceptions.CommandException;
import jimmy.mcgymmy.logic.parser.ParserUtil;
import jimmy.mcgymmy.logic.parser.parameter.Parameter;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.model.ReadOnlyMcGymmy;
import jimmy.mcgymmy.storage.JsonMcGymmyStorage;

public class ImportCommand extends Command {

    public static final String COMMAND_WORD = "import";
    public static final String SHORT_DESCRIPTION = "Import a McGymmy Save file";

    public static final String MESSAGE_IMPORT_FOOD_SUCCESS = "Imported %s";
    public static final String FILE_CONSTRAINTS = "Please select a valid .json file";

    private Parameter<Path> fileParameter = this.addParameter(
            "filepath",
            "",
            "File path to the McGymmy save file",
            "C:/McGymmy/data",
            ParserUtil::parseFile
    );

    void setParameters(Parameter<Path> fileParameter) {
        this.fileParameter = fileParameter;
    }

    @Override
    public CommandResult execute(Model model) throws CommandException {
        Path filepath = fileParameter.consume();
        JsonMcGymmyStorage importedMcgymmy = new JsonMcGymmyStorage(filepath);
        try {
            Optional<ReadOnlyMcGymmy> readOnlyMcGymmyOptional = importedMcgymmy.readMcGymmy();
            if (readOnlyMcGymmyOptional.isEmpty()) {
                throw new CommandException("File is invalid");
            }
            model.setMcGymmy(readOnlyMcGymmyOptional.get());
            return new CommandResult(String.format(MESSAGE_IMPORT_FOOD_SUCCESS, filepath.getFileName()));
        } catch (DataConversionException e) {
            throw new CommandException("Invalid data in file");
        }
    }
}