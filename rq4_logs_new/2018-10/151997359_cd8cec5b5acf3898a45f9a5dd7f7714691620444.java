package ttt;

import java.util.Random;

public class Algorithm implements Comparable<Algorithm> {
	int winCount = 0;
	int tieCount = 0;
	double turnAverage = 0;
	int turnTotal = 0;
	int gamesPlayed = 0;
	Random rnd = new Random();
	
	public Move getMove(int[][] board, int boardToPlayOn, String validBoards) {
		return new Move(boardToPlayOn, 0);
	}

	public void reportResult(int won, int turns) {
		if (won == 1) {
			winCount++;
		} else if (won == 0) {
			tieCount++;
		}
		
		gamesPlayed++;
		
		turnTotal += turns;
		
		turnAverage = turnTotal / gamesPlayed;
	}
	
	
	@Override
	public int compareTo(Algorithm a) {
		if (winCount > a.winCount) {
			return -1;
		} else if (winCount == a.winCount) {
			if (tieCount > a.tieCount) {
				return -1;
			} else if (tieCount == a.tieCount) {
				if (turnAverage < a.turnAverage) {
					return -1;
				} if (turnAverage == a.turnAverage) {
					return 0;
				}
			}
		}
		return 1;
	}
}