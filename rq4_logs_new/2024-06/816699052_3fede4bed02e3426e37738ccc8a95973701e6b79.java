import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;

public class Hangman {
    Scanner scan = new Scanner(System.in);
    

    public static String[] words = {"ant", "baboon", "badger", "bat", "bear", "beaver", "camel",
    "cat", "clam", "cobra", "cougar", "coyote", "crow", "deer",
    "dog", "donkey", "duck", "eagle", "ferret", "fox", "frog", "goat",
    "goose", "hawk", "lion", "lizard", "llama", "mole", "monkey", "moose",
    "mouse", "mule", "newt", "otter", "owl", "panda", "parrot", "pigeon", 
    "python", "rabbit", "ram", "rat", "raven","rhino", "salmon", "seal",
    "shark", "sheep", "skunk", "sloth", "snake", "spider", "stork", "swan",
    "tiger", "toad", "trout", "turkey", "turtle", "weasel", "whale", "wolf",
    "wombat", "zebra"};

    public static String[] gallows = {"+---+\n" +
    "|   |\n" +
    "    |\n" +
    "    |\n" +
    "    |\n" +
    "    |\n" +
    "=========\n",

    "+---+\n" +
    "|   |\n" +
    "O   |\n" +
    "    |\n" +
    "    |\n" +
    "    |\n" +
    "=========\n",

    "+---+\n" +
    "|   |\n" +
    "O   |\n" +
    "|   |\n" +
    "    |\n" +
    "    |\n" +
    "=========\n",

    " +---+\n" +
    " |   |\n" +
    " O   |\n" +
    "/|   |\n" +
    "     |\n" +
    "     |\n" +
    " =========\n",

    " +---+\n" +
    " |   |\n" +
    " O   |\n" +
    "/|\\  |\n" + //if you were wondering, the only way to print '\' is with a trailing escape character, which also happens to be '\'
    "     |\n" +
    "     |\n" +
    " =========\n",

    " +---+\n" +
    " |   |\n" +
    " O   |\n" +
    "/|\\  |\n" +
    "/    |\n" +
    "     |\n" +
    " =========\n",

    " +---+\n" +
    " |   |\n" +
    " O   |\n" +
    "/|\\  |\n" + 
    "/ \\  |\n" +
    "     |\n" +
    " =========\n"};

    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);

        System.out.println("Welcome to hangman! Press any key to start. ");
        scan.nextLine();

        char[] randomWord = randomCharWord();
        String secretWord = new String(randomWord);
        char[] guessedLetters = new char[randomWord.length]; // Dit is om het aantal letters te tellen dat in randomwoord zit
        Arrays.fill(guessedLetters, '_');
        int wrongGuesses = 0;
        char[] missedGuesses = new char[6]; // Het aantal foute gokken is niet groter dan de gallows lenght en vandaar maar 6

        while (wrongGuesses < 6) {
            System.out.println("\n" + gallows[wrongGuesses]);
            System.out.print("Word: \t"); 
            displayGuessedLetters(guessedLetters);
            System.out.print("\n\nMisses: ");
            printMissedGuesses(missedGuesses);

            System.out.print("\n\nGuess a letter: ");
            String input = scan.nextLine();
            if (!isGuessOneLetter(input)) {
                continue;
            }
            char guess = input.charAt(0);
            guess = Character.toLowerCase(guess);   //Om ervoor te zorgen dat je van de invoer van een hoofdletter een kleine letter maakt. Voor de aesthetic 
            
            if (!Character.isLetter(guess)) {
                System.out.println("Please enter a valid letter :(");
                continue;
            }



            if (isLetterGuessed(guess, guessedLetters, missedGuesses)) {
                System.out.println("You already guessed that letter, please try again!");
                continue;
            } 

            if (checkGuess(secretWord, guess)) {
                updatePlaceholders(secretWord, guessedLetters, guess);
            } else {
                missedGuesses[wrongGuesses++] = guess; 
            }

            if (Arrays.equals(secretWord.toCharArray(), guessedLetters)) {
                System.out.println("Congrats! You guessed the word: " + secretWord);
                System.out.println("Your person is now free to walk!");
                System.out.println(
                                    " O     \n" +
                                    "/|\\   \n" + 
                                    "/ \\   \n" );
                break;
            }

            if (wrongGuesses == 6) {
                System.out.print(gallows[6]);
                System.out.println("\nToo bad, you couldn't prevent the person from hanging!");
                System.out.println("\nThe secret word was: '" + secretWord + "' ");
                break;
            }
        }

        scan.close();
    }


    public static String randomWord() {
        Random random = new Random(); // Andere vorm van Math.random waarin je meer controle hebt over de totale lengte
        int index = random.nextInt(words.length); // Hierin pakt hij een random nummer van de maximale lengte van words
        return words[index]; 
    }

    public static char[] randomCharWord() { 
        String randomWord = randomWord();
        return randomWord.toCharArray();
    }

    public static void displayGuessedLetters(char[] guessedLetters) {
        for (char letter : guessedLetters) {
            System.out.print(letter  + " ");
        }
        System.out.println();
    }

    public static void displayPlaceholders(String randomWord, char[] guessedLetters) {
        for (int i = 0; i < randomWord.length(); i++) { // Voor elke letter in het randomwoord print hij nu een _
            guessedLetters[i] = '_';
            System.out.print(guessedLetters[i] + " ");
            }
    }

    public static void printMissedGuesses(char[] wrongGuesses) {
        for (int i = 0; i < wrongGuesses.length; i++) {
            System.out.print(wrongGuesses[i] + " ");
        }
    }

    public static boolean checkGuess(String randomWord, char guess) {
        for (int i = 0; i < randomWord.length(); i++) {
            if (randomWord.charAt(i) == guess) {
                return true;
            }
        } return false;
    }

    public static void updatePlaceholders(String secretWord, char[] guessedLetters, char guess) {
        for (int i = 0; i < secretWord.length(); i++) {
            if (secretWord.charAt(i) == guess) {
                guessedLetters[i] = guess;
            }
        }
    }

    public static boolean isLetterGuessed(char guess, char[] guessedLetters, char[] missedGuesses) {
        for (char letter : guessedLetters) {
            if (letter == guess) {
                return true;
            }
        }
        for (char letter : missedGuesses) {
            if (letter == guess) {
                return true;
            }
        }

        return false;
    }

    public static boolean isGuessOneLetter (String guess) {
        if (guess.length() > 1) {
            System.out.println("\n\nPlease only put one character in");
            return false;
        } else if (guess.length() == 0) { 
            System.out.println("\n\nPlease put in a character");
            return false;
        } return true;
        
    }



}