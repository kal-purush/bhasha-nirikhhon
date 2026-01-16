package com.anconet.gengrpr.validation;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;

public class InputValidator implements IInputValidator {

	@Override
	public boolean validate(CommandLine commandLine) {
		return (commandLine.hasOption("f") && commandLine.hasOption("g") && commandLine.hasOption("i")
				&& requiredInputIsValid(commandLine) && optionalInputIsValid(commandLine));
	}
	
	private static boolean requiredInputIsValid(CommandLine commandLine) {
		return isReadableAsFile(commandLine.getOptionValue("f")) && isNumber(commandLine.getOptionValue("g"))
				&& isNumber(commandLine.getOptionValue("i"));
	}

	private static boolean optionalInputIsValid(CommandLine commandLine) {

		if (commandLine.hasOption("b") && !isReadableAsFile(commandLine.getOptionValue("b"))) {
			return false;
		}
		if (commandLine.hasOption("r") && !isNumber(commandLine.getOptionValue("r"))) {
			return false;
		}
		return true;
	}

	private static boolean isNumber(String optionValue) {
		return optionValue.matches("-?(0|[1-9]\\d*)");
	}

	private static boolean isReadableAsFile(String path) {
		return Files.exists(Paths.get(path)) && Files.isReadable(Paths.get(path));
	}
}