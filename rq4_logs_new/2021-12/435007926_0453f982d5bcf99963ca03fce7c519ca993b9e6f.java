package day3;

import java.util.ArrayList;

public class Power {
	int gamma;
	int epsilon;
	int totalPower;

	int totalPower(int gamma, int epsilon) {
		int total = gamma * epsilon;
		return total;
	}

	int calculateGamma(ArrayList<String> input) {
		int gamma = 0;
		String tempGamma = "";
		System.out.println(input);
		for (int i = 0; i < input.size(); i++) {
			System.out.printf("Byte is: %s \n", input.get(i));
			int one = 0;
			int zero = 0;
			for (int x = 0; x < input.get(i).length(); x++) {
				String character = Character.toString(input.get(i).charAt(x));
				System.out.printf("\nSingle Character is: %s", character);
				if (character.equals("1")) {
					System.out.print("\nPlus One!");
					one++;
				} else if (character.equals("0")) {
					System.out.print("\nPlus Zero!");
					zero++;
				} else {
					System.out.print("\nERROR: not one or zero!");
				}

			}
			if (one > zero) {
				tempGamma = tempGamma + "1";
			} else if (zero > one) {
				tempGamma = tempGamma + "0";
			} else {
				System.out.println("ERROR!");
			}
			one = 0;
			zero = 0;
			System.out.printf("\ntempGamma:  %s \n", tempGamma);
		}
		return gamma;
	}

}