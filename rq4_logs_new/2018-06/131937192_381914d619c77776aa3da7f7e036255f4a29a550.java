package org.vashonsd.Games.HA;

import org.vashonsd.Utils.Minigame;
import java.util.Random;
import java.util.Scanner;
import java.util.HashMap;

public class RockPaperScissors extends Minigame {
    String[] rps = new String[]{"Rock", "Paper", "Scissor"};
    Scanner in = new Scanner(System.in);
    String userChoice = in.next();
    Random rand = new Random();
    String computerChoice = rps[rand.nextInt(rps.length)];
    HashMap<String, Integer> rpslookup = new HashMap<String, Integer>();
    int[][] WinnerTable = new int[3][3];
    int userScore ;
    int computerScore;
    int finalScore;



    public RockPaperScissors() {
        super("RPS", "A simple game of rock, paper, scissors", "quit");
        // Rock = 0, Paper = 1, Scissors = 2
        WinnerTable[0][0] = 0;  // 0 == nobody  wins
        WinnerTable[0][1] = 2;  // 2 == second player wins
        WinnerTable[0][2] = 1;
        WinnerTable[1][1] = 0;
        WinnerTable[1][0] = 1;  // 1 == first player wins
        WinnerTable[1][2] = 2;
        WinnerTable[2][0] = 1;
        WinnerTable[2][1] = 2;
        WinnerTable[2][2] = 0;



        rpslookup.put("ROCK", 0);
        rpslookup.put("PAPER", 1);
        rpslookup.put("SCISSORS", 2);
    }


    @Override
    public String start() {
        return "The game just starte\n Chooes: Rock, paper, or scissor?";
    }

    @Override
    public String handle(String userChoice) {
        computerChoice = rps[rand.nextInt(rps.length - 1)];
        int p1 = rpslookup.get(computerChoice.toUpperCase());
        int p2;
        if (rpslookup.containsKey(userChoice.toUpperCase())) {
            p2 = rpslookup.get(userChoice.toUpperCase());
        } else {
            return "I don't understand " + userChoice + "; I need Rock, Paper or Scissors";
        }

        int whowins = WinnerTable[p1][p2];
        while (finalScore < 5) {
            if (whowins == 0) {

                return "You chose " + userChoice.toUpperCase() + "," + " The computer chose " + computerChoice.toUpperCase() + ". It's a tie!. " +userScore+" - "+computerScore;

            } else if (whowins == 1) {
                computerScore++;
                finalScore++;
                return "You chose " + userChoice.toUpperCase() + "," + " The computer chose " + computerChoice.toUpperCase() + ". The computer win!. " +userScore+" - "+computerScore;
            } else if (whowins == 2) {
                userScore++;
                finalScore++;
                return "You chose " + userChoice.toUpperCase() + "," + " The computer chose " + computerChoice.toUpperCase() + ". You win!. " +userScore+" - "+computerScore;
            }
            return "Try again!";
        }
        if(userScore>computerScore){
            return "You won the game! "+userScore+" - "+computerScore;
        }
        return "You lost!"+userScore+" - "+computerScore;
    }


    @Override
    public String quit() {
        return "Good Bye.";
    }
}