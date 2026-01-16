import java.util.Scanner;

public class HighLow {
    public static void main(String[] args) {
        /*
        Game picks a random number between 1 and 100.
        Prompts user to guess the number.
        All user inputs are validated.
        If user's guess is
            less than the number, it outputs "HIGHER".
            more than the number, it outputs "LOWER".
            the number, the game should declare "GOOD GUESS!"
        */
        int randoNum = randomNumGen();
        System.out.print("Guess a number between 1 and 100: ");
        int guess = validateChoice(1, 100);
        winOrLose(randoNum, guess);
    }
    public static int randomNumGen(){
        int num = (int)(Math.random()*100) + 1;
        return num;
    }

    public static int validateChoice(int min, int max){
        Scanner scanner = new Scanner(System.in);
        int checkInt = scanner.nextInt();

        if(checkInt < min || checkInt > max){
            System.out.println("You have not entered an acceptable value.");
            System.out.printf("Please enter a whole number between %d and %d: ", min, max);
            return validateChoice(min,max);
        }else {
            return checkInt;
        }
    }

    public static void guessAgain(int mark){
        System.out.print("Guess again: ");
        int newGuess = validateChoice(1, 100);
        winOrLose(mark, newGuess);
    }

    public static void winOrLose(int mark, int guess){
            if (guess < mark){
                System.out.print("Higher\n");
                guessAgain(mark);
            }else if(guess > mark){
                System.out.print("Lower\n");
                guessAgain(mark);
            }else {
                System.out.print("Good guess!");
            }
    }
}