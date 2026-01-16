import java.util.Random;
import java.util.Scanner;

public class MotusGame {

    public static void main(String[] args) {
        Scanner s = new Scanner(System.in);

        do {
            playGame();
            System.out.println("Voulez-vous jouer à nouveau ? (oui/non)");
        } while (s.next().equalsIgnoreCase("oui"));

        System.out.println("Merci d'avoir joué !");
    }

    private static void playGame() {
        final int max = 7;
        int attempts = 0;

        String word = generateRandomWord();

        System.out.println("Bienvenue dans le jeu Motus !");
        System.out.println("Essayez de deviner le mot de 8 lettres.");

        while (attempts < max) {
            System.out.println("Entrez votre proposition de mot (8 lettres en majuscules):");
            String guess = getUserInput().toUpperCase();

            if (guess.length() != 8) {
                System.out.println("Veuillez entrer un mot de 8 lettres.");
                continue;
            }

            int[] result = checkGuess(word, guess);
            displayFeedback(result);

            if (result[0] == 8) {
                System.out.println("Félicitations, vous avez trouvé le mot !");
                return;
            }

            attempts++;
            System.out.println("Il vous reste " + (max - attempts) + " essais.");
        }

        System.out.println("Désolé, vous avez utilisé tous vos essais. Le mot était : " + word);
    }

    private static String generateRandomWord() {
        Random r = new Random();
        StringBuilder word = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            char letter = (char) ('A' + r.nextInt(26));
            word.append(letter);
        }
        return word.toString();
    }

    private static String getUserInput() {
        Scanner s = new Scanner(System.in);
        return s.next();
    }

    private static void displayFeedback(int[] result) {
        System.out.println("Lettres bien placées : " + result[0]);
        System.out.println("Lettres mal placées : " + result[1]);
    }

    private static int[] checkGuess(String word, String guess) {
        int[] result = new int[2]; 
        for (int i = 0; i < 8; i++) {
            if (guess.charAt(i) == word.charAt(i)) {
                result[0]++;
            } else if (word.contains(String.valueOf(guess.charAt(i)))) {
                result[1]++;
            }
        }
        return result;
    }
}