package com.kodilla.rps;

import java.util.Random;
import java.util.Scanner;

public class RpsRunner {

    private String playerName;
    private String roundsNumber;
    private String playerMove;
    private int computerMove;
    private int playerScore;
    private int computerScore;

    public void askForNameAndRoundsNumber() {
        Scanner in = new Scanner(System.in);
        System.out.println("Hello, what's your name? ");
        playerName = in.nextLine();
        System.out.println("Welcome to the game " + playerName +
                "! \n How many rounds would you like to play? Type a number from 1 to 10: ");
        while(true) {
            roundsNumber = in.nextLine();
            if(roundsNumber.equals("1") || roundsNumber.equals("2") || roundsNumber.equals("3")
                    || roundsNumber.equals("4") || roundsNumber. equals("5") || roundsNumber.equals("6")
                    || roundsNumber.equals("7") || roundsNumber.equals("8") || roundsNumber.equals("9")
                    || roundsNumber.equals("10")) {
                System.out.println("Chosen number of rounds: " + roundsNumber + ". Let's start!\n");
                break;
            } else {
                System.out.println("Oops! \"" + roundsNumber + "\" is not an integer from 1 to 10. " +
                        "Type a number from 1 to 10.");
            }
        }
    }

    public void showRules() {
        System.out.println("Rules of the game are simple. \n" +
                "Every round you and your computer simultaneously choose to play rock, paper or scissors.\n" +
                "Rock crushes scissors, paper covers rock and scissors cut paper.\n\n" +
                "To play use your keyboard. During the game press:\n\n" +
                "1 to play rock,\n" +
                "2 to play paper,\n" +
                "3 to play scissors.\n\n" +
                "After the game is over, press:\n\n" +
                "x to finish playing or\n" +
                "n to start a new game.\n");
    }

    public void play() {

        boolean end = false;
        int roundCounter = 0;
        Random randomMove = new Random();
        playerScore = 0;
        computerScore = 0;
        int roundsNumberAsInt = Integer.parseInt(roundsNumber);

        while (!end) {

            roundCounter++;
            System.out.println("Round " + roundCounter);

            Scanner in = new Scanner(System.in);
            System.out.println("Your turn! You play: ");

            while(true) {
                playerMove = in.nextLine();
                if (playerMove.equals("1")) {
                    System.out.println("You played rock.");
                    break;
                } else if (playerMove.equals("2")) {
                    System.out.println("You played paper.");
                    break;
                } else if (playerMove.equals("3")) {
                    System.out.println("You played scissors.");
                    break;
                } else {
                    System.out.println("Oops! \"" + playerMove + "\" is not admissible. " +
                            "Press 1 for rock, 2 for paper or 3 for scissors. Try again: ");
                }
            }

            computerMove = randomMove.nextInt(3) + 1;
            if(computerMove == 1) {
                System.out.println("Computer played rock.");
            } else if (computerMove == 2) {
                System.out.println("Computer played paper.");
            } else {
                System.out.println("Computer played scissors.");
            }

            int playerMoveAsInt = Integer.parseInt(playerMove);

            if(playerMoveAsInt == computerMove) {
                System.out.println("Round " + roundCounter + " finished with a draw.");
                playerScore++;
                computerScore++;
            } else if((playerMoveAsInt == 1 && computerMove == 3) || (playerMoveAsInt == 2 && computerMove == 1)
                    || (playerMoveAsInt == 3 && computerMove == 2)) {
                System.out.println("You won round " + roundCounter + "!");
                playerScore++;
            } else {
                System.out.println("Computer won round " + roundCounter + ".");
                computerScore++;
            }

            System.out.println("After round " + roundCounter + " your score is " + playerScore +
                    ". Computer's score is " + computerScore + ".\n");

            if(roundCounter == roundsNumberAsInt) {
                end = true;
            }

        }

    }

    public void showResult() {

        System.out.println("GAME OVER");

        if(playerScore == computerScore) {
            System.out.println("Result: a draw!");
        } else if(playerScore > computerScore) {
            System.out.println("You're the winner!");
        } else {
            System.out.println("Computer won!");
        }

        System.out.println("Your final score: " + playerScore + "\nComputer's final score: " + computerScore);

        System.out.println("\n Would you like to play again? Type n to start a new game or x to quit.\n");
        Scanner in = new Scanner(System.in);
        while (true) {
            String decision = in.next();
            if (decision.equals("x")) {
                System.out.println("Your game is over. Thank you for playing, " + playerName +
                        "." + "See you next time!");
                break;
            } else if (decision.equals("n")) {
                RpsRunner.main(null);
                break;
            } else {
                System.out.println("Oops! I don't understand \"" + decision + "\". " +
                        "Please type n for a new game or x to quit.");
                break;
            }
        }
    }

    public static void main(String[] args) {

        RpsRunner rpsRunner = new RpsRunner();

        rpsRunner.askForNameAndRoundsNumber();
        rpsRunner.showRules();
        rpsRunner.play();
        rpsRunner.showResult();
    }

}