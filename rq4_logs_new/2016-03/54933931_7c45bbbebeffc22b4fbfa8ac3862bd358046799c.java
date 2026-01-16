package warmup5;

//Import library to use Scanner object
import java.util.*;

public class HomeMortguageCalculator {

	// Create a class variable of a Scanner object to allow user input to
	// console
	static Scanner scanner = new Scanner(System.in);

	public static void main(String[] args) {
		// Variables used in this application
		String user_name;
		int property_id;
		int principle;
		float interest;
		int month_term;

		// Ask the user for their name
		System.out.println("Hello, what is your name?");
		user_name = scanner.nextLine();

		// Greet the user
		System.out.println("Thank you " + user_name + ", I hope you are having a good day.");

		// Ask for the property id
		System.out.println("Please tell me the property ID for the property you are interested in?");
		String input = scanner.nextLine();
		property_id = Integer.parseInt(input);
		System.out.println("Thank you " + user_name + ". Okay, so the property ID is " + property_id + ".");

		// Ask for the principle
		System.out.println(user_name + ", please tell me the principle for " + property_id + "?");
		input = scanner.nextLine();
		principle = Integer.parseInt(input);
		System.out.println("Okay, so for property ID " + property_id + " your principle is " + principle + ".");

		// Ask for the interest
		System.out.println("Thank you " + user_name + " Please tell me the interest on the loan?");
		input = scanner.nextLine();
		interest = Float.parseFloat(input);
		System.out.println("Okay " + user_name + ", On property ID " + property_id + " with principle of " + principle
				+ " you have an interest rate of " + interest + "%.");

		// Ask for the term of the loan
		System.out.println("One last question, " + user_name + ".");
		System.out.println("What is the monthly term on the loan?");
		input = scanner.nextLine();
		month_term = Integer.parseInt(input);

		// Now echo all the collected information back to the user
		System.out.println("Okay, here is what I have:");
		System.out.println("Your name is " + user_name + ".");
		System.out.println(
				"The property ID is " + property_id + " with a principle of " + principle + " and an interest rate of "
						+ interest + " and you have a monthly term of " + month_term + " months.");

		// Inform the user what the payments per month are based on what was
		// provided
		double monthlyPayment = (principle * interest) / (1 - Math.pow(1 + interest, -month_term));
		System.out.println("Your monthly payments will be " + monthlyPayment + " .");
	}
}