package week7;

public class nqueen {
	static int answer = 0;
	public static boolean check(int col, int row, int[] board) {
		for (int i = 0; i < col; i++) {
			if (board[i] == row)
				return false;
			if (Math.abs(col - i) == Math.abs(row - board[i]))
				return false;
		}
		return true;
	}
	public static void backtracking(int col, int n, int[] board) {
		if(col == n) {
			answer++;
			return;
		}
		for (int row = 0; row < n; row++) {
			if (check(col, row, board)) {
				board[col] = row;
				backtracking(col + 1, n, board);
			}
		}
  }
	public static int solution(int n) {
		int[] board = new int[n];
		backtracking(0, n, board);
		return answer;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(solution(12));
	}
}