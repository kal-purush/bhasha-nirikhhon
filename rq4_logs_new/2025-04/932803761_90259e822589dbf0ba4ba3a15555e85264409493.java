package uk.ac.bangor.cs.cambria.AcademiGymraeg.util;

import java.util.Arrays;
import java.util.stream.Collectors;

import uk.ac.bangor.cs.cambria.AcademiGymraeg.enums.Gender;

public class GenderService {
	public static String getCommaSeperatedGenders() {
		return Arrays.stream(Gender.values()).map(g -> properCase(g.name())).collect(Collectors.joining(", "));
	}

	private static String properCase(String input) {
		if (input == null || input.isEmpty()) {
			return input;
		}

		StringBuilder sb = new StringBuilder(input.length());
		boolean nextCharCapital = true;

		for (char c : input.toCharArray()) {
			if (Character.isWhitespace(c) || c == '-') {
				nextCharCapital = true;
				sb.append(c);
			} else if (nextCharCapital) {
				sb.append(Character.toUpperCase(c));
				nextCharCapital = false;
			} else {
				sb.append(Character.toLowerCase(c));
			}
		}

		return sb.toString();

	}
}