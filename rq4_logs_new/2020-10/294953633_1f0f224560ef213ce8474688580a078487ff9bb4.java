package jimmy.mcgymmy.logic.macro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import jimmy.mcgymmy.logic.commands.CommandExecutable;
import jimmy.mcgymmy.logic.commands.CommandResult;
import jimmy.mcgymmy.logic.commands.exceptions.CommandException;
import jimmy.mcgymmy.logic.parser.PrimitiveCommandParser;
import jimmy.mcgymmy.logic.parser.exceptions.ParseException;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.model.macro.Macro;

public class MacroRunner {

    public static CommandExecutable asCommandInstance(Macro macro, CommandLine args) {
        return model -> executeWith(model, args, substituteAll(args, macro.getRawCommands()));
    }

    /**
     * Executes the macro on the model.
     * Returns upon encountering a help or exit function
     * (i.e. does not attempt to execute any command
     * after encountering help or exit)
     *
     * @param model Model to run the macro's commands on.
     * @param args  arguments to the macro.
     * @return CommandResult produced by concatenating the messages from all the executed commands.
     * @throws CommandException if any command encounters an error.
     */
    public static CommandResult executeWith(Model model, CommandLine args, String[] substitutedCommands)
            throws CommandException {
        List<String> messagesToUser = new ArrayList<>();
        List<CommandExecutable> commandExecutables = parseCommands(substitutedCommands);
        int lastCommandIndex = 0;
        try {
            for (lastCommandIndex = 0; lastCommandIndex < commandExecutables.size(); lastCommandIndex++) {
                CommandResult result = commandExecutables.get(lastCommandIndex).execute(model);
                if (result.isExit() || result.isShowHelp()) {
                    return result;
                }
                messagesToUser.add(result.getFeedbackToUser());
            }
            return new CommandResult(String.join("\n", messagesToUser));
        } catch (CommandException e) {
            /* note: not factoring out code below because its only used here and its
               purpose/what it's doing is obvious, and factoring it out will be very messy. */
            String errorLocation = "\n\nAn error occurred when executing this command:\n"
                    + substitutedCommands[lastCommandIndex];
            String doneCommands = lastCommandIndex == 0 ? ""
                    : "\n\nThe following commands executed successfully:\n"
                    + String.join("\n", Arrays.copyOfRange(substitutedCommands, 0, lastCommandIndex));
            String notDoneCommands = lastCommandIndex + 1 == substitutedCommands.length ? ""
                    : "\n\n The following commands were not executed:\n"
                    + String.join("\n",
                            Arrays.copyOfRange(substitutedCommands, lastCommandIndex + 1, substitutedCommands.length));

            String message = e.getMessage() + errorLocation + doneCommands + notDoneCommands;
            throw new CommandException(message);
        }
    }

    private static String[] substituteAll(CommandLine args, String[] rawCommands) {
        String[] output = new String[rawCommands.length];
        for (int i = 0; i < rawCommands.length; i++) {
            output[i] = substitute(args, rawCommands[i]);
        }
        return output;
    }

    private static List<CommandExecutable> parseCommands(String[] rawCommands) throws CommandException {
        PrimitiveCommandParser parser = new PrimitiveCommandParser();
        List<CommandExecutable> commandExecutables = new ArrayList<>();
        try {
            for (String rawCommand : rawCommands) {
                commandExecutables.add(parser.parse(rawCommand));
            }
            return commandExecutables;

        } catch (ParseException e) {
            // TODO better error message
            throw new CommandException("Macro failed to parse: " + e.getMessage());
        }
    }

    private static String substitute(CommandLine args, String line) {
        // TODO possibly use a StringBuilder here instead for performance.
        for (Iterator<Option> i = args.iterator(); i.hasNext(); ) {
            Option option = i.next();
            String from = option.getOpt();
            String to = args.getOptionValue(from);
            line = line.replaceAll("\\\\" + from, to);
        }

        String unusedArgs = String.join(" ", args.getArgList());
        // regex is matching for "\$".
        return line.replaceAll("\\\\\\$", unusedArgs).trim();
    }
}