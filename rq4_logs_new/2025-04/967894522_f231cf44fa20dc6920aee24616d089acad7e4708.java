import java.util.Scanner;

public class Calculator {
	public static void main(String[] args) {
		// For user input
		Scanner input = new Scanner(System.in);


		double num1 = 0, num2 = 0;
		char operator;

		// Get valid number inputs from the user
		while (true) {
			// Prompt for the first number
			System.out.print("Enter the first number: ");
			if (!input.hasNextDouble()) {
				System.out.println("Invalid input. Please enter a valid number.");
				input.next(); // Discard invalid input
				continue;
			}
			num1 = input.nextDouble();

			// Prompt for the second number
			System.out.print("Enter the second number: ");
			if (!input.hasNextDouble()) {
				System.out.println("Invalid input. Please enter a valid number.");
				input.next(); // Discard invalid input
				continue;
			}
			num2 = input.nextDouble();
			break;
		}

		// get valid operator input for user while handling typos
		while (true) {
			System.out.print("Enter an operator (+, -, *, /): ");
			String inputOp = input.next();

		
			if (inputOp.length() == 1 && "+-*/".contains(inputOp)) {
				operator = inputOp.charAt(0);
				break;
			} else {
				System.out.println("Invalid operator. Please enter one of +, -, *, /.");
			}
		}

		// calculates the result
		double result = switch (operator) {
			case '+' -> num1 + num2;
			case '-' -> num1 - num2;
			case '*' -> num1 * num2;
			case '/' -> {
				// Check for divide-by-zero, yields to return results from switch
				if (num2 == 0) {
					System.out.println("Error: Cannot divide by zero.");
					yield Double.NaN;
				} else {
					yield num1 / num2;
				}
			}
			default -> {
				// should never run due to earier error handling
				System.out.println("Error: Invalid operator.");
				yield Double.NaN;
			}
		};

		// Display the result
		System.out.println("Result: " + result);

		// Close the Scanner as best practice
		input.close();
	}
}