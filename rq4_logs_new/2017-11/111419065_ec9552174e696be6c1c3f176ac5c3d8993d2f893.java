import java.util.Scanner;

public class GamePlayer
{
		
	private GuessingGame guessingGame;	
	private Scanner player;	
	public GamePlayer(Scanner player) {
		Scanner user = player; 
		// TODO construct me
	}

	public String play() {
		System.out.println("1.  make a guess.");
      System.out.println("2.  get a hint.");
		System.out.println("3.  print statistics.");
      System.out.println("4.  quit this game.");
      int input = player.nextInt();
      String s = "";
      switch (input)
      {
         case 1:
         {
          System.out.println("Please input your guess");
          guessingGame.makeGuess(player.nextInt());
          s = "";    
         }break;
         case 2:
         {
           s = guessingGame.hint();
         }break;
         case 3:
         {
            s = guessingGame.toString();
         }break;
         case 4:
         {
            s = "Thanks For Playing";
            guessingGame.quit();
         }break;
      }
      return s;
	}

}
