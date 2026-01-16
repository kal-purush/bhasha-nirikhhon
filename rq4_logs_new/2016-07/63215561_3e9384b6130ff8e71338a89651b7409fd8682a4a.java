package Assignment1;


import java.util.Scanner;

public class BinaryToOctal {
	static int indexOfNextTriplet = 0;
	static int binaryNumber = 0;// binary number to be converted
	static String octalNumber = "";// octal number

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Scan the user input
		System.out.println("Enter the number");
		Scanner scanner = new Scanner(System.in);
		binaryNumber = scanner.nextInt();

		// call method for conversion
		int output=new BinaryToOctal().convertBinaryToOctal(binaryNumber);
		System.out.println(binaryNumber+" in octal is "+octalNumber);

	}

	public int convertBinaryToOctal(int n) {

		int lengthOfInput = (n + "").length();
		String binaryNumberCorrected = "" + binaryNumber;
		BinaryToOctal object = new BinaryToOctal();
		
		//if the input number is not in form of triplets then add 
		//corresponding zeroes before the number
		if ((lengthOfInput % 3) != 0) {
			binaryNumberCorrected = object.PrefixZeroes(binaryNumber + "");
		}
		
		//calculate the octal equivalent for each triplet and find the final number
		while (indexOfNextTriplet < binaryNumberCorrected.length()) {
			octalNumber += object
					.getOctalNumberforTriplet(binaryNumberCorrected.substring(
							indexOfNextTriplet, indexOfNextTriplet + 3));
			indexOfNextTriplet += 3;
		}
		return Integer.parseInt(octalNumber);
	}

	//caculates the octal equivalent for each triplet in binary form 
	int getOctalNumberforTriplet(String input) {
		int number1 = Integer.parseInt(input.charAt(0) + "");
		int number2 = Integer.parseInt(input.charAt(1) + "");
		int number3 = Integer.parseInt(input.charAt(2) + "");
		int octalNumber = 0;

		if (number1 == 1) {
			octalNumber += 4;
		} else {
			octalNumber += 0;
		}
		if (number2 == 1) {
			octalNumber += 2;
		} else {
			octalNumber += 0;
		}
		if (number3 == 1) {
			octalNumber += 1;
		} else {
			octalNumber += 0;
		}
		return octalNumber;

	}

	//adds correct number of zeroes before the number to make the number in triplets 
	public String PrefixZeroes(String input) {
		input = "0" + input;
		if ((input.length() % 3) == 0) {
			return input;
		} else {
			input = PrefixZeroes(input);
			return input;
		}

	}

}