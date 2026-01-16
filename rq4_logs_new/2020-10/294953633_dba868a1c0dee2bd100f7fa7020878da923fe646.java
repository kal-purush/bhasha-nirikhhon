package jimmy.mcgymmy.logic.commands;

import jimmy.mcgymmy.logic.commands.exceptions.CommandException;
import jimmy.mcgymmy.logic.parser.parameter.OptionalParameter;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.model.macro.Macro;
import jimmy.mcgymmy.model.macro.MacroList;

public class ListMacroCommand extends Command {
    public static final String COMMAND_WORD = "listmacro";
    public static final String SHORT_DESCRIPTION = "List all macros in McGymmy.";

    private final OptionalParameter<String> specificCommand = this.addOptionalParameter(
            "which",
            "",
            "List details of a specific macro",
            "deleteMatching"
    );

    @Override
    public CommandResult execute(Model model) throws CommandException {
        MacroList macroList = model.getMacroList();

        if (specificCommand.getValue().isEmpty()) {
            return new CommandResult(listAllMacros(macroList));
        }

        String macroName = specificCommand.getValue().get();

        if (!macroList.hasMacro(macroName)) {
            throw new CommandException(macroName + " is not an existing macro.");
        }

        return new CommandResult(formatMacro(macroList.getMacro(macroName)));
    }

    private String listAllMacros(MacroList macroList) {
        StringBuilder sb = new StringBuilder("Here are all the available macros.");
        sb.append("\n\nType '").append(COMMAND_WORD).append(" [MACRO_NAME]' for more info on a specific macro.\n\n");
        for (Macro macro : macroList.getAsList()) {
            sb.append(macro.getName()).append("\n");
        }
        return sb.toString();
    }

    private String formatMacro(Macro macro) {
        StringBuilder sb = new StringBuilder("Information on macro ").append(macro.getName()).append(":");
        sb.append("\nParameters: ").append(String.join(", ", macro.getMacroArguments()));
        sb.append("\nDeclaration: ").append(String.join("; ", macro.getRawCommands()));
        return sb.toString();
    }
}