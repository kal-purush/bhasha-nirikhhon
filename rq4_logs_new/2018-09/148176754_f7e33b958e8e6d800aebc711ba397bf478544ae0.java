
//make sure we can use scanners
import java.util.Scanner;

public class WeekOneFortuneTeller {

	public static void main(String[] args) {
		// create a new scanner to get us some sweet, sweet keyboard input.
		// let's refer to this scanner as "input"
		Scanner input = new Scanner(System.in);

		// First let's gobble up some data like we're facebook
////////////////////////////////////////DATA GATHERING STAGE////////////////////////////////////////////////////
		// Ask the user an innocent question by displaying it in the console
		System.out.println(
				"Prepare to have your fortune actually predicted.\nIf you'd like to quit, just type \"quit\" instead of an answer."
						+ "\nFirst, please type your first name, then press enter.");
		// Let's save their response in a String type variable called firstName
		String firstName = input.nextLine();
		// BOOM, one step closer to stealing their identity!
		// Save that info for later when you can sell it to facebook.
		// Also, let's check to see if they told us their name was quit, in which case
		// let's back off and quit the program.
		WeekOneFortuneTeller.checkQuit(firstName);
		// aaaand repeat. Next question.
		System.out.println("Great, how about your last name?");
		String lastName = input.nextLine();
		WeekOneFortuneTeller.checkQuit(lastName);
		//
		System.out.println("Now, not to get too personal, but how old are you?");
		// let's save this as a string for now
		String ageString = input.nextLine();
		// check to see if the user wants to quit at THIS input. Exit's coming up,
		// buddy. You want off?
		WeekOneFortuneTeller.checkQuit(ageString);
		// now let's turn that into an integer from a string. Or at least try.
		int age;
		// let's not let the user break the program by telling us our age is G or some
		// shit.
		try {
			age = Integer.parseInt(ageString);
		} catch (NumberFormatException ex) {
			System.out.println(
					"You cannot be an amount of years old that is not a number.\nWe're going to say you're 10,000. Nice going, Methuselah.");
			age = 10000;
		}

		// I want the user to be able to write the month's name besides just giving the
		// number.
		System.out.println("And if you could be born in any month you were actually born in, what would that month be?"
				+ "\nYou can write the name or its number, but no abbreviations like Oct or Jan. May is alright.");
		String birthMonthString = input.nextLine();
		WeekOneFortuneTeller.checkQuit(birthMonthString);
		birthMonthString = birthMonthString.toLowerCase();
		// let's just a switch case to go through our options based on the input we
		// received.
		// if they typed out a month, let's parse that.
		switch (birthMonthString) {
		case "january":
		case "1":
		case "01":
			birthMonthString = "1";
			break;
		case "february":
		case "2":
		case "02":
			birthMonthString = "2";
			break;
		case "march":
		case "3":
		case "03":
			birthMonthString = "3";
			break;
		case "april":
		case "4":
		case "04":
			birthMonthString = "4";
			break;
		case "may":
		case "5":
		case "05":
			birthMonthString = "5";
			break;
		case "june":
		case "6":
		case "06":
			birthMonthString = "6";
			break;
		case "july":
		case "7":
		case "07":
			birthMonthString = "7";
			break;
		case "august":
		case "8":
		case "08":
			birthMonthString = "8";
			break;
		case "september":
		case "9":
		case "09":
			birthMonthString = "9";
			break;
		case "october":
		case "10":
			birthMonthString = "10";
			break;
		case "november":
		case "11":
			birthMonthString = "11";
			break;
		case "december":
		case "12":
			birthMonthString = "12";
			break;
		default:
			birthMonthString = "2";
			System.out.println("Did you misspell something? Let's just say you were born in February.");
		}
		// then let's turn that string into an integer so we can operate on it later.
		int birthMonth = Integer.parseInt(birthMonthString);

		// And now we're going to ask about their favorite color:
		System.out.println(
				"Out of the ROY G. BIV colors, which is your favorite? If you don't know what these are, type HELP");
		String favColor = input.nextLine();
		WeekOneFortuneTeller.checkQuit(favColor);

		// we'll use this variable to see if they've already asked for help because
		// we're going to be snarky the first time.
		int timesAsked = 0;
		// so, this loop will run as long as they keep saying HELP is their favorite
		// color.
		while (favColor.equalsIgnoreCase("HELP")) {
			// if this is the first time they ask for help, give them snark.
			if (timesAsked == 0) {
				System.out.println("You know, the ROG. B IVY Colors.");
				timesAsked++;
				favColor = input.nextLine();
				WeekOneFortuneTeller.checkQuit(favColor);
				// okay, maybe they actually need help.
			} else if (timesAsked == 1) {
				timesAsked++;
				System.out.println(
						"I mean Red, Orange, Yellow, Green, Blue, Indigo, or Violet. That's who ROY G. BIV is.");
				favColor = input.nextLine();
				WeekOneFortuneTeller.checkQuit(favColor);
			} else if (timesAsked == 2) { // why are they asking again
				timesAsked++;
				System.out.println("ROY G BIV stands for: Red, Orange, Yellow, Green, Blue, Indigo, Violet.");
				favColor = input.nextLine();
				WeekOneFortuneTeller.checkQuit(favColor);
			} else if (timesAsked == 3) { // why are they asking again
				timesAsked++;
				System.out.println(
						"Like, the R is for Red. O is Orange. And so on. Indigo is like, a purple kind of blue.");
				favColor = input.nextLine();
				WeekOneFortuneTeller.checkQuit(favColor);
			} else {
				System.out.println(
						"You could say red. Or orange. Or yellow. Green is fine. As are blue and violet, aka purple.\nAlso acceptable is indigo. Please type one of these.");
				favColor = input.nextLine();
				WeekOneFortuneTeller.checkQuit(favColor);
			}

			// if has asked conditional end
		} // while loop end

		// back to normal questions
		System.out.println("Finally, how many siblings do you have?");
		String siblingsString = input.nextLine();
		WeekOneFortuneTeller.checkQuit(siblingsString);
		int siblings;
		try {
			siblings = Integer.parseInt(siblingsString);
		} catch (NumberFormatException ex) {
			System.out.println("The number of siblings you have should be a number. Let's just say there's 2 of them.");
			siblings = 2;
		}

		// alright, we have their info. Let's make some shit up.
		// if their age is even or odd, we might force them into early retirement.
		String retirement;
		if (age % 2 == 0) {
			retirement = "3,000 years";
		} else {
			retirement = "4,000 years";
		}

		// the location of their vacation home (which is a 100% normal thing to have) is
		// based on how many of their siblings are chipping in.
		String vacationHome = "vacation home error"; // this OUGHT to be overwritten. If it isn't, you've managed to
														// have a number of siblings I didn't account for.
		if (siblings < 0) {
			vacationHome = "in the vacuumm of space, where you quickly suffocate amongst the slendor of the cosmos.\nDid you kill your siblings? Is that what happened!? Why are there less than none!?";
		} else if (siblings == 0) {
			vacationHome = "adjacent to your normal home. What could be more practical? Saves so much gas.";
		} else if (siblings == 1) {
			vacationHome = "on the banks of Steverson Lake, Nebraska. It's not that bad, and you got a really good deal on the real estate.\nAnd nobody is ever asking to come visit you, so you get that solitude.";
		} else if (siblings == 2) {
			vacationHome = "a vast complex of linked hot air baloons, floating over Porcupine, South Dakota.\nYou've been designing this for years. They said you couldn't do it.\nBut you could.";
		} else if (siblings == 3) {
			vacationHome = "constantly in peril of being \"borrowed for the weekend\" by your siblings.";
		} else if (siblings > 3 && siblings < 12) { // I feel like more than this and the user is lying.
			vacationHome = "in the Akmola Province of Kazakhstan. It beautiful and far, far away from your many, many siblings.";
		} else { // so you have more than 10 siblings? Really?
			vacationHome = "in an underground bunker. Your army of siblings has tried to draft you into a weird militia.\nYou're on the run by this point. This is your only refuge.";
		}

		// so now we're gonna decide their bank based on their birth month.
		String bankBalance = "bank balance error";
		if (birthMonth > 0 && birthMonth < 5) {
			bankBalance = "six dollars but the actual Mona Lisa in a safe deposit box in Switzerland.";
		} else if (birthMonth > 4 && birthMonth < 9) {
			bankBalance = "$125 for appearance's sake, but you actually keep your money in gold bars\nunder your mattress because the government.";
		} else if (birthMonth > 8 && birthMonth < 13) {
			bankBalance = "all the money in the world. All of it. Your super villain plan worked, but in the process\nyou obviously destabilized the economy and all that currency is worthless.\nThe bank laughs at you as they trade their livestock for grain on the bartering market.";
		} else {
			bankBalance = "the secrets of the universe, considering you were born outside of time.";
		}

		// How you get around is going to be dependent on your favorite color. People
		// might fuck around with their spellings so let's make this a little more
		// robust:
		// let's just get the first character from their favorite color. Maybe they read
		// ROY G BIV and were like, O is the literal thing you are asking for, program.
		char col = favColor.charAt(0);
		// then let's make that character into a string because equalsIgnoreCase didn't
		// immediately work how I wanted to with chars.
		String color = Character.toString(col);
		// then based on the letter of your favorite color, here's your transportation.
		String car = "car error";
		if (color.equalsIgnoreCase("r")) {
			car = "an actual rocket ship. It's useless for going to Kroger, but not to get to Moon Kroger!";
		} else if (color.equalsIgnoreCase("o")) {
			car = "eight reindeer hitched together like you're motherfuckin' Santa Claus";
		} else if (color.equalsIgnoreCase("y")) {
			car = "a puma you've caught, trained, and made a cute little saddle for. It is fearsome.";
		} else if (color.equalsIgnoreCase("g")) {
			car = "your own feet. You don't need any fancy automobile when you've got two cars on the end of your legs!";
		} else if (color.equalsIgnoreCase("b")) {
			car = "a submarine. The world has flooded by this point. The grocery store is in a kelp forest.";
		} else if (color.equalsIgnoreCase("i")) {
			car = "an infinite number of brick masons who are constantly restructuring the built environment to move around you.\nYou don't go to the store. The store comes to you. All movement is an illusion, but yours is especially.";
		} else if (color.equalsIgnoreCase("v")) {
			car = "a dog sled team, but instead of dogs it's norwegian forest cats and instead of a sled it's a surfboard.\nYou're pretty bodacious in your old age.";
		} else { // because you always need a contingency plan for if they didn't answer
					// correctly. Ugh. Hypothetical users.
			car = "equipped with six GPS devices but it doesn't matter becuase you don't follow directions anyway.";
		}

		// and now we'll spit out the fortune we've computed. This is how magic works.
		// We've done a spell. Wizards.
		System.out.println(
				"\n\n\n" + firstName + " " + lastName + ". It has been " + retirement + ". You have finally retired.");
		System.out.println("In your bank account, you will have " + bankBalance);
		System.out.println("Your vacation home will be " + vacationHome);
		System.out.println("Your mode of transportation will be " + car);
		if (firstName.equalsIgnoreCase("Brian") || firstName.equalsIgnoreCase("Donnie")) {
			System.out.println(
					"You will one day look back on the time you gave a great grade to an entertaining\ncoding project as a pivitol moment in your career.");
		}
		System.out.println("Enjoy your retirement.");

		input.close();

	}

	public static void checkQuit(String lingo) {
		if (lingo.equalsIgnoreCase("QUIT")) {
			System.out.println("Ain't nobody like a quitter.\nBut fine.");
			System.exit(0);
		}
	}

}