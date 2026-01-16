package com.kodilla.rps;

import java.util.*;

public class Battle {
    public int isWon(int userMove, int computerMove) {
        //   int[userMove][computerMove]
        int[][] winningDiagram = new int[3][3];
        // result is 1=win , 0=draw, -1=lose
        winningDiagram[0][0] = 0;
        winningDiagram[0][1] = -1;
        winningDiagram[0][2] = 1;
        winningDiagram[1][0] = 1;
        winningDiagram[1][1] = 0;
        winningDiagram[1][2] = -1;
        winningDiagram[2][0] = -1;
        winningDiagram[2][1] = 1;
        winningDiagram[2][2] = 0;
        return winningDiagram[userMove-1][computerMove-1];
    }

    public int computerChoiceLevelHard(int userChoice) {
        List<Integer> answerList = new ArrayList<>();
        final Random generator = new Random();
        int won50percent = 0;
        int loose25percent = 0;
        int draw25percent = 0;

        //1-kamień, 2-papier, 3-nożyczki
        if (userChoice == 1) {
            won50percent = 2;
            loose25percent = 3;
            draw25percent = 1;
        }
        if (userChoice == 2) {
            won50percent = 3;
            loose25percent = 1;
            draw25percent = 2;
        }
        if (userChoice == 3) {
            won50percent = 1;
            loose25percent = 2;
            draw25percent = 3;
        }
        for(int i = 0; i <25; i++) {
            answerList.add(loose25percent);
            answerList.add(won50percent);
            answerList.add(draw25percent);
            answerList.add(won50percent);
        }
        Collections.shuffle(answerList);
        return answerList.get(generator.nextInt(answerList.size()));
    }
}