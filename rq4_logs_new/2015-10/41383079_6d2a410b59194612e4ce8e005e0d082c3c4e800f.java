class Solution {
	/*
	 * Fluery Algorithm, traversal complete graph's each edge with only one time
	 */
	Stack<Integer> stk = new Stack<>();
	
	private void dfs(int[][] edge, int x, int n) {
		stk.push(x);
		for (int i = 0; i < n; i++) {
			if (edge[x][i] > 0) {
				edge[x][i] = 0;
				dfs(edge, i, n);
				break;
			}
		}
	}
	
	private void fluery(int[][] edge, int x, int n) {
		stk.push(x);
		while (!stk.isEmpty()) {
			boolean next = false;
			int cur = stk.peek();
			for (int i = 0; i < n; i++) {
				if (edge[cur][i] > 0) {
					next = true;
					break;
				}
			}
			if (!next) {
				System.out.print(stk.pop());
			}
			else {
				int y = stk.pop();
				dfs(edge, y, n);
			}
		}
	}
	
	public void get(int n) {
		int[][] edge = new int[n][n];
		for (int j = 0; j < n; j++) {
			for (int i = 0; i < n; i++) {
				if (j != i) {
					edge[j][i] = 1;
				}
			}
		}
		fluery(edge, 0, n);
	}
}