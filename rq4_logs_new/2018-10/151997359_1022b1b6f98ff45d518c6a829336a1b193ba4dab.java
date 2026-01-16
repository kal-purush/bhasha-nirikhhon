package ttt;

public class BigBoard {
	public SmallBoard[] boards = new SmallBoard[9];
	private SmallBoard tracking_board = new SmallBoard();
	private char current_board;

	public BigBoard() {
	    for (int i = 0; i < 9; i++) boards[i] = new SmallBoard();
    }

	private boolean isTaken(int pos, int board) {
		if (board == -1) board = current_board;
		return boards[board].isTaken(pos);
	}

	private int getValue(int pos, int board) {
		if (board == -1) board = current_board;
		return boards[board].getValue(pos);
	}

	private boolean takeSpot(int b, int pos, boolean o) {
		if (isTaken(pos, b)) return false;
		if (boards[b].takeSpot(pos, o)) {
			int solving = boards[b].isSolved();
			if (solving != SmallBoard.BOARD_VALUE_UNTAKEN) tracking_board.takeSpot(b, solving == SmallBoard.BOARD_VALUE_O);
			current_board = (char)pos;
			return true;
		}
		return false;
	}

	private int isSolved() {
		return tracking_board.isSolved();
	}

	public void makeMove(char player, Move move) {
		takeSpot(move.bIndex, move.sIndex, player == 'O');
	}

	public String checkForWin() {
		int solv = isSolved();
		if (solv == SmallBoard.BOARD_VALUE_UNTAKEN) return "NOT COMPLETE";
		else if (solv == SmallBoard.BOARD_VALUE_X) return "X";
		else return "O";
	}

	public int count(SmallBoard[] list, char player) {
		int c = 0;
		for (int i = 0; i < list.length; i++) {
			if (list[i].checkForWin() == String.valueOf(player)) {
				c++;
			}
		}
		return c;
	}

    public String getFairBoards() {
        String res = "";

        for (int i = 0; i < 9; i++) {
            if (boards[i].checkForWin().length() != 1) {
                res += String.valueOf(i);
            }
        }

        return res;
    }

	public int[][] makeBoardForPlayer(char player) {
		int[][] boardsArr = new int[9][9];

		for (int i = 0; i < 9; i++) {
		    int[] b;
		    assert boards[i] != null;
		    assert boards[i].getBoardForPlayer(player) != null;
            try {
                b = boards[i].getBoardForPlayer(player);
            } catch (NullPointerException e) {
                System.err.printf("%d is null: 1\n", i);
                System.exit(1);
                return boardsArr;
            }
            try {
                boardsArr[i] = b;
            } catch (NullPointerException e) {
                System.err.printf("%d is null: 2\n", i);
                System.exit(1);
            }
		}

		return boardsArr;
	}
};