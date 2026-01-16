package ttt;

import java.util.ArrayList;

public class BigBoard {
	ArrayList<SmallBoard> smallBoards = new ArrayList<SmallBoard>();
	
	public BigBoard() {
		ArrayList<Character> emptyCharBoard = new ArrayList<Character>();
		
		for(int i = 0; i < 9; i++) {
			emptyCharBoard.add(' ');
		}
		
		for(int i = 0; i < 9; i++) {
			smallBoards.add(new SmallBoard(emptyCharBoard, i));
		}
	}
	
	public void makeMove(char player, int bigIndex, int smallIndex) {
		smallBoards.get(bigIndex).placeMark(player, smallIndex);
	}
	
	public String checkForWin() {
		//TIE
		SmallBoard[] all = {smallBoards.get(0), smallBoards.get(1), smallBoards.get(2), smallBoards.get(3), smallBoards.get(4), smallBoards.get(5), smallBoards.get(6), smallBoards.get(7), smallBoards.get(8)};
		if (count(all, ' ') == 0) {
			return "TIE";
		}
		//Top Row
		SmallBoard[] top = {smallBoards.get(0), smallBoards.get(1), smallBoards.get(2)};
		if (count(top, 'X') == 3) {
			return "X";
		} else if (count(top, 'O') == 3) {
			return "O";
		}
		
		//horz Row
		SmallBoard[] horz = {smallBoards.get(3), smallBoards.get(4), smallBoards.get(5)};
		if (count(horz, 'X') == 3) {
			return "X";
		} else if (count(horz, 'O') == 3) {
			return "O";
		}
		
		//Bottom Row
		SmallBoard[] bottom = {smallBoards.get(6), smallBoards.get(7), smallBoards.get(8)};
		if (count(bottom, 'X') == 3) {
			return "X";
		} else if (count(bottom, 'O') == 3) {
			return "O";
		}
		
		//Left Row
		SmallBoard[] left = {smallBoards.get(0), smallBoards.get(3), smallBoards.get(6)};
		if (count(left, 'X') == 3) {
			return "X";
		} else if (count(left, 'O') == 3) {
			return "O";
		}
		
		//Vert Row
		SmallBoard[] vert = {smallBoards.get(1), smallBoards.get(4), smallBoards.get(7)};
		if (count(vert, 'X') == 3) {
			return "X";
		} else if (count(vert, 'O') == 3) {
			return "O";
		}
		
		//Right Row
		SmallBoard[] right = {smallBoards.get(2), smallBoards.get(5), smallBoards.get(8)};
		if (count(right, 'X') == 3) {
			return "X";
		} else if (count(right, 'O') == 3) {
			return "O";
		}
		
		//Left Diag
		SmallBoard[] leftDiag = {smallBoards.get(0), smallBoards.get(4), smallBoards.get(8)};
		if (count(leftDiag, 'X') == 3) {
			return "X";
		} else if (count(leftDiag, 'O') == 3) {
			return "O";
		}
		
		//Right Diag Row
		SmallBoard[] rightDiag = {smallBoards.get(2), smallBoards.get(4), smallBoards.get(6)};
		if (count(rightDiag, 'X') == 3) {
			return "X";
		} else if (count(rightDiag, 'O') == 3) {
			return "O";
		}
		
		return "NOT COMPLETE";
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
	
}