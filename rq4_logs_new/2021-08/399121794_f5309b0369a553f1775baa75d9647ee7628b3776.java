import java.util.ArrayList;
import java.util.Scanner;

/**
 * Driver for chips game. Creates player classes to play chips game.
 * 
 * @author Kai
 * @author Mr. Kiang (revised, cleaned up game code, added comments)
 * @see ChipsPlayer ChipsPlayer
 */
public class ChipsDriver{
	
	/**
	 * main method
	 */
	public static void main(String[] args)
	{
		ChipsPlayer player1; // Declares a player1 and player2 object. Because these objects extend ChipsPlayer (they are a type of ChipsPlayer)
		ChipsPlayer player2; // we know we are setting aside enough room no matter which student's object it is.
		int initpile = 100; 
		int pile = 100;
		int lastMove = 0;
		int numgames = 0;
		int p1wins = 0; // For win stats
		int p2wins = 0;
		int p1forfeit = 0; // How many games player forfeits because of illegal plays 
		int p2forfeit = 0;
		int randomMax = 0; // If random pile size chosen, what the max is
		int nextPlay = 0;
		String playerError = " made an invalid play.";
		String p1name = ""; // Created so we can change the name of the player if the same class is chosen for both players
		String p2name = "";
		boolean showText = true; // Whether the moves are displayed on screen
		boolean player1turn = true;
		boolean error = false; // Whether an illegal move was made
		boolean randomPile = false; // Whether a random pile size is chosen each game
		ArrayList<ChipsPlayer> players = new ArrayList<ChipsPlayer>(); // These are ArrayLists to store all of the potential players
		ArrayList<ChipsPlayer> players2 = new ArrayList<ChipsPlayer>(); // Make sure the class files are located in the same folder as this driver file.
		
		//*
		//add new players here in the same format
		players.add(new ChipsPlayer_Human());
		players2.add(new ChipsPlayer_Human());
		players.add(new ChipsPlayer());
		players2.add(new ChipsPlayer());
		players.add(new ChipsPlayer_MrK());
		players2.add(new ChipsPlayer_MrK());
		players.add(new ChipsPlayer_HasKnowledge());
		players2.add(new ChipsPlayer_HasKnowledge());
		players.add(new ChipsPlayer_Simone());
		players2.add(new ChipsPlayer_Simone());
		players.add(new ChipsPlayer_AkisWilliam());
		players2.add(new ChipsPlayer_AkisWilliam());
		//*
		
		Scanner in = new Scanner(System.in);
		
		//Setting Pile Size
		System.out.println("Pick a pile size (0 = Random)");
		initpile = in.nextInt();
		
		//random pile size
		if (initpile == 0) // If player has chosen to have a pile size that varies
		{
			System.out.println("Random pile size range: 3 to ___?"); // This sets the maximum for the pile and stores it in randomMax
			randomMax = in.nextInt();
			randomPile = true;
		}
		
		//listing players for p1 selection
		System.out.println("Please select first player (player 1 goes first)");    
		for(int i = 0; i < players.size(); i++)
		{
			System.out.print(i + "."+players.get(i)+"\n"); // This iterates through the entire Players arraylist and grabs the name of each Player from each object's toString()
		}
		player1 = players.get(in.nextInt());
		
		//listing players for p2 selection
		System.out.println("Please select second player");    
		for(int i = 0; i < players2.size(); i++)
		{
			System.out.print(i + "." + players2.get(i) + "\n");
		}
		player2 = players2.get(in.nextInt());
		
		//number of games
		System.out.println("How many games?");
		numgames = in.nextInt();
		
		//game text
		System.out.println("Show game text? (true/false)");
		showText = in.nextBoolean();
		System.out.println();
		p1name = player1.toString();
		p2name = player2.toString();
		if (p1name.equals(p2name))
		{
			p1name = "Good " + p1name;
			p2name = "Evil " + p2name; // If two instances of the same object are played off against each other, one is the evil twin
		}
		//Main Game Loop
		for (int i = 1; i <= numgames; i++) // This repeats the following code for the number of specified games.
		{
			pile = initpile; // Sets pile size to user-specified number, unless...
			if (randomPile) // if user has chosen to make the piles be random,
				pile = (int)(Math.random()*(randomMax-3))+3; // choose a random pile size between 3 (the minimum) and randomMax (the maximum).
			lastMove = 1;
			player1turn = true; // At the start of each game player 1 goes first
			boolean firstMove = true;
			System.out.println("\nGame No. " + i + " (" + pile + " chips)");
//*** START OF EACH GAME ***
			while (pile > 0) 
			{
				if (showText)
					System.out.println("Pile size = " + pile);
				if (player1turn) // If it's player1's turn call player1 move
				{
					
					//System.out.println("P1 play(" + pile + "," + lastMove + ")"); // DEBUG code, uncomment this if you want to see the actual call being made.
					
					nextPlay = player1.makeMove(pile, firstMove, lastMove); // This calls the Player object's play() method passing in the pile size and the last move. A lastMove value of 0 signifies to the player that there was no previous move
					if (showText) // If user wants to know...
						System.out.println(p1name+" took " + nextPlay + " chips."); // Print the move.
				}
				else
				{
					//System.out.println("P2 play(" + pile + ", " + lastMove + ")"); // DEBUG code, uncomment this if you want to see the actual call being made.
					nextPlay = player2.makeMove(pile, false, lastMove); // otherwise call player2 move
					if (showText)
						System.out.println(p2name+" took " + nextPlay + " chips.");
				}
				// If any illegal moves occurred...
				if ((!firstMove && (nextPlay > 2 * lastMove)) || // If it's not the first move, and the player takes more than twice what the last player took, or
						(firstMove && (nextPlay > pile - 1)) || // if it's the first move and the player tries to take it all, or
						nextPlay < 1) // if the player takes less than 1 piece under any circumstances
				{
					pile = 0; // then this game is over!
					error = true; // Flag the game as an error
					if (player1turn) // Print appropriate error message based on whose turn it is
					{
						System.out.println(p1name + " made an illegal play.");
						System.out.println(p2name + " wins!");
						p2wins++;
						p1forfeit++;
					}
					else 
					{
						System.out.println(p2name + " made an illegal play.");
						System.out.println(p1name + " wins!");
						p1wins++;
						p2forfeit++;
					}
				}
				
				else // Otherwise, update pile and lastMove accordingly 
				{
					lastMove = nextPlay;
					pile -= lastMove;
				}
				firstMove = false;
				player1turn = !player1turn; // flips turns
			} 
//*** END OF EACH GAME ***
			if (error) // If there was an illegal move we have already forfeited and counted the win, so reset the error flag and ignore below.
				error = false;
			else
			{
				if (player1turn == false) // If the pile is zero and we have flipped the player1turn flag and it is false, then player 1 made the last move
				{
					System.out.println(p1name + " wins!");
					p1wins++;
				}
				else
				{
					System.out.println(p2name + " wins!");
					p2wins++;
				}
			}
		}
		//End of main loop (all games completed)
		//Print win record for each player
		
		String pileSizeText = ""+initpile;
		if (initpile == 0) pileSizeText = "Random from 3-"+randomMax;
		System.out.println("\nPile size:"+pileSizeText+" Games:"+numgames+"\n"+p1name+" won "+p1wins+" times (went first)\n"+p2name+" won "+p2wins+" times");
		if (p1forfeit > 0) System.out.println(p1name + " forfeited " + p1forfeit + " game(s)."); // If there were any forfeited games due to illegal moves
		if (p2forfeit > 0) System.out.println(p2name + " forfeited " + p2forfeit + " game(s)."); // they are reported here.
		
		/* There is a way to make this print out nicely in a table format but I don't exactly have it correct.
		 System.out.format("%10s%5s%5s","Name","Wins","%");
		 System.out.println();
		 System.out.format("%10s%5d%5f",p1name,p1wins,p1wins/(double)numgames);
		 System.out.println();
		 System.out.format("%10s%5d%5f",p2name,p2wins,p2wins/(double)numgames);
		 */
	}
}