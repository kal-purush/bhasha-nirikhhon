import java.util.*;

class BagelsRunner {
  public BagelsRunner() {
    // not much to do here
  }
  public void runBagels () {
    boolean eating = true;
    while ( eating ) {
      Bagels b = new Bagels ( "123" );
      System.out.println ( "Try guessing: " );
      Scanner s = new Scanner ( System.in );
      String guess = s.nextLine();
      boolean correct = b.checkCombo ( guess );
      if ( correct ) {
        System.out.println ( "You're right!" );
        System.out.println ( "Want to play again? Y or N" );
        Scanner c = new Scanner ( System.in );
        char playAgainResponse = c.next().charAt(0);
        if ( playAgainResponse == 'N' || playAgainResponse == 'n' ) {
          eating = false;
          System.out.println ( "Bye!" );
        } else {
          System.out.println ( "Rebooting..." );
        }
      } else {
        System.out.println ( "No, but here are some clues:" );
        System.out.println ( b );
      }
    }
  }
  public static void main ( String [] args ) {
      System.out.println ( "simpleBagels Java" );
      System.out.println ( "by Jeffrey Wang" );
      System.out.println ( "All guesses are numbers." );
      System.out.println ( "! means the position is wrong, ? means the number is not within the array." );
      BagelsRunner b = new BagelsRunner();
      b.runBagels();
  }
}