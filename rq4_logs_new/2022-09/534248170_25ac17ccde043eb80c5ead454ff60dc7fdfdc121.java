import java.io.IOException;
import java.util.Scanner;
import java.util.Random;

public class BoatRacing
{
	public static void main(String[] args) throws IOException
	{
		while (true)
		{
			//Create objects used for running the game
			Scanner input = new Scanner(System.in);
			SeaObject s0 = new SeaObject();
			Processor p;
			DisplayGame display = new DisplayGame();
			String choice;
			String p1name;
			String p2name;
			String limitp1;
			String limitp2;

			//try...catch ensures inputs are only integers
			try
			{
				display.mainMenu();
				System.out.print("\nEnter option: ");
				choice = input.nextLine();
				Integer.parseInt(choice);

				while (Integer.parseInt(choice) < 1 || Integer.parseInt(choice) > 3)
				{
					System.out.println("\n==================================================");
					System.out.println("     Error! Please enter an option displayed!     ");
					System.out.println("==================================================\n");
					display.mainMenu();
					System.out.print("\nEnter option: ");
					choice = input.nextLine();
				}

				//Runs the correct function according to user's input
				switch (choice)
				{
					//Runs the game if user enters 1
					case "1":
						//Display key and welcome message
						display.gameRules();

						//Requests names for player 1 and player 2
						do
						{
							System.out.println("Enter name for player 1 (max 10 characters): ");
							p1name = input.nextLine().toLowerCase();
							System.out.println("Enter name for player 2 (max 10 characters): ");
							p2name = input.nextLine().toLowerCase();
							if (p1name.length() > 10)
							{
								limitp1 = p1name.substring(0, 10);
								p1name = limitp1;
							}
							if (p2name.length() > 10)
							{
								limitp2 = p2name.substring(0, 10);
								p2name = limitp2;
							}
						} while (p1name.equals("") || p2name.equals(""));

						//Creates player objects for both player 1 and 2 in both p object and BoatRacing class
						p = new Processor(p1name, p2name);
						Player p1 = new Player(p1name);
						Player p2 = new Player(p2name);

						//Displays both players' names
						System.out.println("P1: " + p1.getPlayerName());
						System.out.println("P2: " + p2.getPlayerName());

						System.out.println("Player 1 Turn");

						while (!p.hasWon())
						{
							s0.setSpecialMove();
							System.out.println("--------------------------------------------------------------------------");
							display.displayGrid(p.getGrid(),p1.getPlayerName(),p2.getPlayerName());
							System.out.println();
							System.out.println("Rolling Dice {1,2,3,4,5,6}");
							p.possibleMove(rngDice(),s0.getSpecialMove(), p1name, p2name);
							System.out.println("--------------------------------------------------------------------------");
							System.out.println("Press ENTER to continue");
							input.nextLine();
							p.setSwitchPlayer();
							//Correctly display the players' position on the grid
							p1.getPlayer1Pos(p.getGrid(), p.getCurrentPlayer(), p1name);
							p2.getPlayer2Pos(p.getGrid(), p.getCurrentPlayer(), p2name);
						}

						if (p.getCurrentPlayer() == 1){
							System.out.println("Player " + p.getCurrentPlayer() + ", " + p1.getPlayerName() + " has won!");
						}
						else {
							System.out.println("Player " + p.getCurrentPlayer() + ", " + p2.getPlayerName() + " has won!");
						}

						p.appendScores();
						try {
							System.out.println();
							display.displayScores();
							System.out.println();
						}
						catch (IOException e){
							System.out.println("Display failed");
						}
						break;

					//Displays the leaderboard if user enters 2
					case "2":
						try {
							System.out.println();
							display.displayScores();
							System.out.println();
						}
						catch (IOException e){
							System.out.println("Display failed");
						}
						break;

					//Prints thank you message if user enters 3
					case "3":
						System.out.println();
						display.thankYou();
						System.out.println();
						break;
				}
				//Ends the game if user enters 3
				if (Integer.parseInt(choice) == 3){
					break;
				}
			}
			catch (NumberFormatException e)
			{
				System.out.println("\n===========================================");
				System.out.println("     Error! Please only enter numbers!     ");
				System.out.println("===========================================\n");
			}
		}
	}
	
	public static int rngDice()
	{
		Random rng = new Random();
		int dice = 1 + rng.nextInt(6);
		System.out.println("Dice: " + dice);
		return dice;
	}
}
