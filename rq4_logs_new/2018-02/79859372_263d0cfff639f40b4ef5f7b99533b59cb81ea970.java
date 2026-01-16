//package cardgame;
//Ben//Ben Olson

//PC
//1-27-17

import java.util.ArrayList;
import java.util.Scanner;


class BlackJackGame
{
	public static void main(String[] args)
	{
		Deck theDeck = new Deck();
		Scanner s = new Scanner(System.in);
		String cmd = " ";
		System.out.println("Welcome to BlackJack! How many Players?");
		int playerNum = s.nextInt();

		while (playerNum <= 0 || playerNum > 5) 
		{
			System.out.println("Unusable amount of players! Please input another amount between 0-5!");
			playerNum = s.nextInt();
			// quick check for within range
		}//End while (playerNum <= 0 || playerNum > 5) 
		
		System.out.print("Please Wait...");
		System.out.print(".");

		System.out.print(".");
		System.out.print(".");

		theDeck.shuffleDeck();

		// Creating a list of player hands
		ArrayList<Player> players = new ArrayList<Player>((playerNum));
		for (int i = 0; i <=(playerNum); i++)
		{
			players.add(new Player());
			dealCardToPlayer(theDeck, players.get(i),i);
			dealCardToPlayer(theDeck, players.get(i),i);
		}

		System.out.println("");
		System.out.println("\nThe deck has been shuffled, the players have been readied. Lets Play!");
		System.out.println("Rules are simple, Play to 21. Go over and you bust, go under and you might lose!");
		System.out.println("Good Luck and Have Fun!");

		//for loop to run through players and deal cards
		/*
		for(int i = players.size()-1; i>=0; i--)
		{
			theDeck.dealCard(players.get(i));
			theDeck.dealCard(players.get(i));
		}
		*/
		
		System.out.println();
		
		System.out.println("Dealer:");
		players.get(0).printHand();
	
		for(int i=players.size()-1;i>0;i--)
		{
			//for loop cycles through players top down
			//because dealer is position zero
			for(int j = 0; j < 20; j++)
				System.out.println("\n");
			
			//if ((i+1) < players.size() && players.get(i + 1).getIsOut())
				//System.out.println("Player " + (i+1) + "  Busted \n\n");

			cmd = " ";
			while((!cmd.equals("stand")) && (!cmd.equals("bust")))
			{
				System.out.println();
				System.out.println("Player "+ i +" Please select either Hit or Stand by typing H or S.");
				System.out.println("Player "+i+"'s Hand value is: " + players.get(i).getHandValue());
				System.out.println("Player "+ i + "'s Hand:");
				players.get(i).printHand();
				cmd = s.next();
				switch(cmd)
				{
					case "s":	
						cmd = "stand"; 
						break;
						
					case "h":
						cmd = dealCardToPlayer(theDeck, players.get(i),i);
						break;
						
					case "S": 	
						cmd = "stand"; 
						break;
						
					case "H": 
						cmd = dealCardToPlayer(theDeck, players.get(i),i);
						break;
						
					default:
						System.out.println("Invaild input...");
						break;
				}
				
				//System.out.println("Player "+i+"'s Hand value is: " + players.get(i).getHandValue());
					
			}
			//String var needs to be set back to a default after player while loop
			System.out.println("Player "+i+"'s Hand is:");
			players.get(i).printHand();
			cmd = " ";
		}
		
		
		System.out.println(" ");
		System.out.println("The players have all played, now its the dealers turn...");
		System.out.println(" ");
		System.out.println("Dealer's Hand is:");
		while(players.get(0).getHandValue()<18)
		{
			//deck.dealCard(players.get(0)); //deal a card to the dealer
			//players.get(0).printHand(); //show the card
			dealCardToPlayer(theDeck, players.get(0),0);

		}
		players.get(0).printHand(); //show the card
		System.out.println("Dealer's hand value is: " + players.get(0).getHandValue());


		// Check players for winner...
		ArrayList<String> winners = new ArrayList<String>();
		ArrayList<String> tie = new ArrayList<String>();
		ArrayList<String> losers = new ArrayList<String>();
		
		for(int i=players.size()-1;i>0;i--)
		{
			if(!players.get(i).getIsOut())
			{
				if(players.get(0).getIsOut())
					winners.add(("Player "+ i + " "));
				else if(players.get(i).getHandValue() > players.get(0).getHandValue())
					winners.add(("Player "+ i + " "));
				else if(players.get(i).getHandValue() == players.get(0).getHandValue())
					tie.add(("Player "+ i + " "));// if player ties with dealer bet is returned...
				else if(players.get(i).getHandValue() < players.get(0).getHandValue())
					losers.add(("Player "+ i + " "));
					
			}
			else 
			{
				losers.add(("Player "+ i + " "));
			}
		}
		
		System.out.println("");
		System.out.println("Winners are: ");
		
		for(String r : winners)
		{
			System.out.println(r);
		} // try to iterate through the arraylists
		
		System.out.println("");
		System.out.println("Ties are: ");
		
		for(String r : tie)
		{
			System.out.println(r);
		}
		
		System.out.println("");
		System.out.println("Losers are: ");
		
		for(String r : losers)
		{
			System.out.println(r);
		}
	}
	
	public static String dealCardToPlayer(Deck deck, Player p,int i)
	{
		deck.dealCard(p);
		if(p.getHandValue()>21)
		{	
			if(i==0)
			{
				System.out.println("Dealer Busts!");
				p.setisOut(true);
				return "bust";
			}
			else
			{
				System.out.println("Player " + i + " Busts!");
				p.setisOut(true);
				return "bust";
			}
			
		}
		return " "; 
	}
	
	static void Game()
	{
		String[] args= new String[6];
		String Q = " ";
		boolean gameStatus=true;
		Scanner s = new Scanner(System.in);
		while(gameStatus == true)
		{
			BlackJackGame.main(args);
			
			System.out.println("Would you like to play again?");
			System.out.println();
			Q = s.next();
			if(Q.equals("no")||Q.equals("n")||Q.equals("NO")||Q.equals("No"))
			{
				gameStatus = false;
				GameServer.main(args);
			} 
		}
		s.close();
	}	
}