import java.util.Scanner;

public class Bob {
    public static void main(String[] args) {
      System.out.println("Regrettably HAL is no longer in service. BOB is now the mainframe interface.\nYou may start your conversation with him at this time.\nSimply type \"quit\" when you're done.\n");
      Scanner sc = new Scanner(System.in);
      String userInput = " ";
      String response = " ";

      do{
          System.out.print(">");
          userInput = sc.nextLine();

          if (userInput.endsWith("?")){
              response = "Sure.";
          }else if(userInput.endsWith("!")) {
              response = "Whoa, chill out!";
          }else if(userInput.length() == 0) {
              response = "Fine. Be that way!";
          }else {
              response = "Whatever.";
          }

          System.out.println(response + "\n");

      }while(!userInput.equalsIgnoreCase("quit"));
      System.out.println("Interface paused. Goodbye");
    }
}