package homeWork.thirdLesson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

public class Main {
    public static void main(String[] args) throws IOException {
        guessTheNumber();
        guessTheWord();
    }

    static void guessTheNumber() throws IOException {
        BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
        int guessNumber = new Random().nextInt(10);
        do {
            System.out.println("Input your guess Number:");
            for(int i = 0; i < 3; i++) {
                int input = Integer.parseInt(buffer.readLine());
                if(guessNumber == input) {
                    System.out.println("You win, Congrats!");
                    System.out.println("Do you want to repeat?(1/0): ");
                    break;
                } else if(guessNumber > input && i < 2) {
                    System.out.println("Your number is smaller! you have " + (3 - (i + 1)) + " chances");
                } else {
                    System.out.println("Your number is bigger! you have " + (3 - (i + 1)) + " chances");
                    if(i == 2) {
                        System.out.println("You lose!");
                        System.out.println("Do you want to repeat?(1/0): ");
                    }
                }
            }

        } while (Integer.parseInt(buffer.readLine()) == 1);
        buffer.close();
    }

    static void guessTheWord() throws IOException {
        String[] words = {"apple", "orange", "lemon", "banana", "apricot", "avocado",
                "broccoli", "carrot", "cherry", "garlic", "grape", "melon", "leak",
                "kiwi", "mango", "mushroom", "nut", "olive", "pea", "peanut", "pear",
                "pepper", "pineapple", "pumpkin", "potato"};
        String guessWord = words[new Random().nextInt(words.length)];
        BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
        String word;
        int counter = 0;
        do {
            System.out.println("Guess the word: ");
            word = buffer.readLine();
            if (!word.equals(guessWord)) {
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < guessWord.length(); i++) {
                    if (guessWord.charAt(i) == word.charAt(i)) {
                        builder.append(guessWord.charAt(i));
                    } else {
                        String mask = "###############";
                        builder.append(mask);
                        System.out.println("You guess only this characters:");
                        System.out.println(builder.toString());
                        break;
                    }
                }
            }
            counter ++;
        } while (!word.equals(guessWord));
        System.out.println("Congrats you guess! Your score is " + counter + " and word is: " + guessWord);
    }
}