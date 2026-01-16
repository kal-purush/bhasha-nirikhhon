package tictactoe.domain.usecases;

import java.util.ArrayList;
import java.util.List;

public class IsWinnerUseCase {

    ArrayList<Integer> allPositionOfGame;
    ArrayList<Integer> playerOnePositions;
    ArrayList<Integer> playerTwoPositions;
    List<Integer> winningPositions = new ArrayList<>();

    public int isWinner(RecordPositionUseCase position) {
        allPositionOfGame = position.getPositions();
        playerOnePositions = position.getPlayerOnePositions();
        playerTwoPositions = position.getPlayeTwoPositions();

        int[][] possibleWinningCombinations = {
            {1, 2, 3}, {4, 5, 6}, {7, 8, 9}, // possible horizontail winning combination
            {1, 4, 7}, {2, 5, 8}, {3, 6, 9}, // possible vertical winning combination
            {1, 5, 8}, {3, 5, 7} // possible diagonal winning combination
        };

        for (int i = 0; i < possibleWinningCombinations.length; i++) { // if playerone has the winning combination
            int position1 = possibleWinningCombinations[i][0];
            int position2 = possibleWinningCombinations[i][1];
            int position3 = possibleWinningCombinations[i][2];

            if (playerOnePositions.contains(position1)
                    && playerOnePositions.contains(position2)
                    && playerOnePositions.contains(position3)) {
                winningPositions.clear(); // Clear previous positions
                winningPositions.add(position1);
                winningPositions.add(position2);
                winningPositions.add(position3);
                return 1;
            }

        }

        for (int i = 0; i < possibleWinningCombinations.length; i++) { // if playerTwo has the winning combination
            int position1 = possibleWinningCombinations[i][0];
            int position2 = possibleWinningCombinations[i][1];
            int position3 = possibleWinningCombinations[i][2];

            if (playerTwoPositions.contains(position1)
                    && playerTwoPositions.contains(position2)
                    && playerTwoPositions.contains(position3)) {
                winningPositions.clear(); // Clear previous positions
                winningPositions.add(position1);
                winningPositions.add(position2);
                winningPositions.add(position3);
                return 2;
            }

        }
        winningPositions.clear();
        return 0; // no one win
    }

    public List<Integer> getWinningPositions() {
        return winningPositions; // Return the winning positions
    }

}