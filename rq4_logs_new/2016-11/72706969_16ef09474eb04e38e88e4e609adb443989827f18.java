package fracCalc;

import java.util.Scanner;

public class FracCalc {
	public static void main(String[] args) {
		// TODO: Read the input from the user and call produceAnswer with an
		// equation
		Scanner console = new Scanner(System.in);
		String input = console.nextLine();
		while (input.compareToIgnoreCase("quit") != 0) {
			String answer = produceAnswer(input);
			System.out.println(answer);
			input = console.nextLine();
		}
		;
		console.close();
	}

	// ** IMPORTANT ** DO NOT DELETE THIS FUNCTION. This function will be used
	// to test your code
	// This function takes a String 'input' and produces the result
	//
	// input is a fraction string that needs to be evaluated. For your program,
	// this will be the user input.
	// e.g. input ==> "1/2 + 3/4"
	//
	// The function should return the result of the fraction after it has been
	// calculated
	// e.g. return ==> "1_1/4"
	public static String produceAnswer(String input) {
		int wholeAnswer = 0;
		int numeratorAnswer = 0;
		int denominatorAnswer = 1;
		// TODO: Implement this function to produce the solution to the input
		Scanner equationScanner = new Scanner(input).useDelimiter(" ");
		String operand1 = equationScanner.next();
		Number number1 = parsingNumbers(operand1);
		String operator = equationScanner.next();
		String operand2 = equationScanner.next();
		Number number2 = parsingNumbers(operand2);
		if (operator.contains("-")) {
			// Whole number Subtraction
			wholeAnswer = number1.whole - number2.whole;
			// Finding common denominators
			if (number1.denominator != number2.denominator) {
				denominatorAnswer = number1.denominator * number2.denominator;
				int numeratorAnswer1 = number1.denominator * number2.numerator;
				int numeratorAnswer2 = number1.numerator * number2.denominator;
				numeratorAnswer = numeratorAnswer1 - numeratorAnswer2;
			} else {
				denominatorAnswer = number1.denominator;
				numeratorAnswer = number1.numerator - number2.numerator;
			}
		}
		if (operator.contains("+")) {
			// Whole number Addition
			wholeAnswer = number1.whole + number2.whole;
			// Finding common denominators
			if (number1.denominator != number2.denominator) {
				denominatorAnswer = number1.denominator * number2.denominator;
				int numeratorAnswer1 = number1.denominator * number2.numerator;
				int numeratorAnswer2 = number1.numerator * number2.denominator;
				numeratorAnswer = numeratorAnswer1 + numeratorAnswer2;
			} else {
				denominatorAnswer = number1.denominator;
				numeratorAnswer = number1.numerator + number2.numerator;
			}
		}
		if (operator.contains("*")) {
			// Whole number Multiplication
			wholeAnswer = number1.whole * number2.whole;
			// Fraction Multiplication
			denominatorAnswer = number1.denominator * number2.denominator;
			numeratorAnswer = number1.numerator * number2.numerator;
		}
		if (operator.contains("/")) {
			// Whole number Division
			if(number2.whole == 0){
				wholeAnswer = 0;
			}
			else{
			wholeAnswer = number1.whole / number2.whole;
			}
			// Fraction Division
			denominatorAnswer = number1.denominator * number2.numerator;
			numeratorAnswer = number1.numerator * number2.denominator;
		}
		//Simplification
		String answerString;
		if(wholeAnswer != 0){
			answerString = wholeAnswer + "_" + Math.abs(numeratorAnswer)+ "/" + Math.abs(denominatorAnswer);
		}
		else{
			answerString = numeratorAnswer+ "/" + denominatorAnswer;
		}
		return answerString;
		
	}

	// TODO: Fill in the space below with any helper methods that you think you
	// will need
	public static Number parsingNumbers(String operand) {
		Scanner wholeScanner = new Scanner(operand).useDelimiter("_");
		int whole = 0;
		int numerator = 0;
		int denominator = 1;
		if (wholeScanner.hasNextInt()) {
			whole = wholeScanner.nextInt();
		}
		if (wholeScanner.hasNext()) {
			String fractionString = wholeScanner.next();
			Scanner fractionScanner = new Scanner(fractionString).useDelimiter("/");
			if (fractionScanner.hasNextInt()) {
				numerator = fractionScanner.nextInt();
			}
			denominator = fractionScanner.nextInt();
		}
		Number number = new Number();
		number.whole = whole;
		if(whole < 0){
		number.numerator = -1 * numerator;
		}
		else{
			number.numerator = numerator;
		}
		number.denominator = denominator;
		return number;
	}
}