package ttt;

public class SmallBoard {
	private char board_values;
	private boolean board_value_last;
	private char board_values_taken;
	private boolean board_value_taken_last;

	public final static int BOARD_VALUE_UNTAKEN = 0;
	public final static int BOARD_VALUE_X = 1;
	public final static int BOARD_VALUE_O = 2;

	public boolean isTaken(int pos) {
		if (pos > 8 || pos < 0) return true;
		else if (pos == 8) return board_value_taken_last;
		else return ((board_values_taken >> pos) & 1) == 1;
	}

	public int getValue(int pos) {
		if (!isTaken(pos) || (pos > 8 || pos < 0)) return BOARD_VALUE_UNTAKEN;
		else if (pos == 8) return board_value_last ? BOARD_VALUE_O : BOARD_VALUE_X;
		else return ((board_values >> pos) & 1) + 1;
	}

	public boolean takeSpot(int pos, boolean o) {
		if (isTaken(pos) || (pos > 8 || pos < 0)) return false;
		else if (pos == 8) {board_value_taken_last = true; board_value_last = o;}
		else {board_values |= ((o ? 1 : 0) << pos); board_values_taken |= (1 << pos);}
		return true;
	}

	public int isSolved() {
		for (int i = 0; i < 3; i++)
			if (getValue(i) == getValue(i+3) && getValue(i+3) == getValue(i+6) && getValue(i) != BOARD_VALUE_UNTAKEN)
				return getValue(i);
		for (int i = 0; i < 7; i+=3)
			if (getValue(i) == getValue(i+1) && getValue(i+1) == getValue(i+2) && getValue(i) != BOARD_VALUE_UNTAKEN)
				return getValue(i);
		if (getValue(0) == getValue(4) && getValue(4) == getValue(8) && getValue(0) != BOARD_VALUE_UNTAKEN)
			return getValue(0);
		else if (getValue(2) == getValue(4) && getValue(4) == getValue(6) && getValue(2) != BOARD_VALUE_UNTAKEN)
			return getValue(2);
		return BOARD_VALUE_UNTAKEN;
	}

	private int charToO(char c) {
		if (c == 'X') return BOARD_VALUE_X;
		else return BOARD_VALUE_O;
	}

	public void placeMark(char player, int index) {
		takeSpot(index, player == 'O');
	}

	public int[] getBoardForPlayer(char player) {
		int[] returnBoard = new int[9];
		for (int i = 0; i < 9; i++) {
			if (getValue(i) == BOARD_VALUE_UNTAKEN)
				returnBoard[i] = 0;
			else if (getValue(i) == charToO(player))
				returnBoard[i] = 1;
			else returnBoard[i] = -1;
		}
		return returnBoard;
	}

	public String checkForWin() {
		int solv = isSolved();
		if (solv == SmallBoard.BOARD_VALUE_UNTAKEN) return "NOT COMPLETE";
		else if (solv == SmallBoard.BOARD_VALUE_X) return "X";
		else return "O";
	}

	public int count(char[] list, char elem) {
		int c = 0;
		for (char aList : list) if (aList == elem) c++;
		return c;
	}
};