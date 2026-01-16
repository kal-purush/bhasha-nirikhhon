import java.util.Random;
import java.util.Scanner;

public class GameImproved {

    public static void main(String[] args) {
        System.out.println("Welcome to the lucky game");

        System.out.println("Please type your first name");

        Scanner scanner = new Scanner(System.in);

        String userName = scanner.next();

        System.out.println("Hi "+userName +", do you want to start?");
        System.out.println("\t 1. Yes");
        System.out.println("\t 2. No");

        int answerValdiation = scanner.nextInt();

        while (answerValdiation==2){
            System.out.println("Hi "+userName +", do you want to start?");
            System.out.println("\t 1. Yes");
            System.out.println("\t 2. No");
            answerValdiation = scanner.nextInt();
        }

        Random random = new Random();
        int randomNumber = random.nextInt(9);

        System.out.println("Guess the number, type a number between 0 to 9");
        int numberReceiver = scanner.nextInt();

        boolean winner = false;
        int timesTrying = 0;
        boolean shouldFinish = false;

        while (!shouldFinish){
            timesTrying++;

            if (timesTrying<5){
                if(numberReceiver==randomNumber){
                    winner= true;
                    shouldFinish=true;
                }else if(randomNumber>numberReceiver){
                    System.out.println("Please try with a higher number");
                    numberReceiver = scanner.nextInt();
                }else if(randomNumber<numberReceiver){
                    System.out.println("Please try with a lower number");
                    numberReceiver = scanner.nextInt();
                }
            }else {
                shouldFinish = true;
            }
        }
        if(winner){
            System.out.println("CONGRATULATIONS");
        }else{
            System.out.println("GAME OVER");
            System.out.println("The number was "+randomNumber);
        }










    }
}