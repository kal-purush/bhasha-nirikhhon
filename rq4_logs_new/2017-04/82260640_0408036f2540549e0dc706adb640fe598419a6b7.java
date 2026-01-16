package example2920;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {

	public static void main(String args[]) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		try {
			String line = br.readLine();
			String[] input = line.split(" ");
			int[] inputNumber = new int[8];

			for (int i = 0; i < 8; i++) {
				inputNumber[i] = Integer.parseInt(input[i]);
			}
			System.out.println(check(inputNumber));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String check(int[] inputNumber) {

		int index = 0;
		while (index < 7) {
			if (inputNumber[index] < inputNumber[index + 1]) {
				index++;
				if (index == 7)
					return "ascending";
			} else {
				break;
			}
		}

		index = 7;
		while (index > 0) {
			if (inputNumber[index] < inputNumber[index - 1]) {
				index--;
				if (index == 0)
					return "descending";
			} else {
				break;
			}
		}

		return "mixed";
	}
}