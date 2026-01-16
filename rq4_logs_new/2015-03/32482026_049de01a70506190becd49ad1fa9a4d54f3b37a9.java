/**
 * Created by ben on 3/18/15.
 */

import java.util.Scanner;

public class Exercises {

    public static void main(String[] args) {
        Grader grader = new Grader();

        // Comment or uncomment the lines below to grade any or all of the exercises.
        grader.gradeExercise("GreaterThan50", GreaterThan50());
        grader.gradeExercise("PotatoPotato",  PotatoPotato());
        grader.gradeExercise("WhileInput",    WhileInput());

        // Print the final results of the test.
        grader.printTotals();
    }

    public static boolean GreaterThan50() {
        /**
         *  Exercise 1:
         *  Using a Scanner, prompt the user for an integer.
         *  Return true if the integer is greater than 50.
         *  Return false if the integer is lesser than 50.
         */

        // Write your solution here.

        return false; // Change this line to return your answer instead of false.
    }

    public static String PotatoPotato() {
        /**
         *  Exercise 2:
         *  Using a for loop, build a string consisting of the string "potato" repeated
         *  five times, with no spaces. Return that string.
         */

        // Write your solution here.

        return ""; // Change this line to return your answer instead of a blank string.
    }

    public static String WhileInput() {
        /**
         *  Exercise 3:
         *  Using a while loop, repeatedly accept strings from the user until the user
         *  inputs a blank string. Return a single string that consists of all the user's
         *  input mashed into one long string. For example, if I input "horse", "battery",
         *  and "staple", return "horsebatterystaple".
         */

        // Write your solution here.

        return ""; // Change this line to return your answer instead of a blank string.
    }
}