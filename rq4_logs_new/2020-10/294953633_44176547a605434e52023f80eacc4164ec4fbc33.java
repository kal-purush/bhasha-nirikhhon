package jimmy.mcgymmy.logic.parser;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.Options;

import jimmy.mcgymmy.logic.parser.parameter.AbstractParameter;
import jimmy.mcgymmy.logic.parser.parameter.ParameterSet;

public class PrimitiveCommandHelpUtil {
    // Creates the usage string using commons-cli's HelpFormatter and the createExampleCommand function
    public static String getUsage(String commandName, ParameterSet parameterSet) {
        Options options = parameterSet.asOptions();
        String formattedHelp = ParserUtil.getUsageFromHelpFormatter(commandName,
                getUnnamedParameterUsage(parameterSet), options);
        return formattedHelp + "\nEXAMPLE: " + createExampleCommand(commandName, parameterSet.getParameterList());
    }

    private static String getUnnamedParameterUsage(ParameterSet parameterSet) {
        return parameterSet.getUnnamedParameter()
                .map(param -> String.format("<arg> %s: %s", param.getName(), param.getDescription()))
                .orElseGet(() -> "");
    }

    private static String createExampleCommand(String commandName, List<AbstractParameter> parameterList) {
        return commandName + " " + parameterList.stream()
                .map(p -> p.getFlag().equals("") ? p.getExample() : "-" + p.getFlag() + " " + p.getExample())
                .collect(Collectors.joining(" "));
    }
}