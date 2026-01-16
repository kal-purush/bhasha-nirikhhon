package Chapter07;

import java.util.Scanner;

/**
 * program finds a high score in a class
 *
 * @author Josh Plato
 */
public class C7_1 {

    /**
     * main method
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        // Create the array
        System.out.print("Enter the number of students: ");
        int[] scores = new int[input.nextInt()];

        // Fill array with scores
        System.out.print("Enter " + scores.length + " scores: ");
        for (int i = 0; i < scores.length; i++) {
            scores[i] = input.nextInt();
        }

        // Determine the highest score
        int highestScore = bestScore(scores);

        // Display output
        for (int i = 0; i < scores.length; i++) {
            System.out.println("Student " + i + " score is " + scores[i] + " and grade is " + determineGrade(scores[i], highestScore));
        }
    }

    public static int bestScore(int[] scores) {
        int best = scores[0];

        for (int i = 1; i < scores.length; i++) {
            if (scores[i] > best) {
                best = scores[i];
            }
        }

        return best;
    }

    public static char determineGrade(int score, int best) {
        if (score >= best - 10) {
            return 'A';
        } else if (score >= best - 20) {
            return 'B';
        } else if (score >= best - 30) {
            return 'C';
        } else if (score >= best - 40) {
            return 'D';
        } else {
            return 'F';
        }
    }
}