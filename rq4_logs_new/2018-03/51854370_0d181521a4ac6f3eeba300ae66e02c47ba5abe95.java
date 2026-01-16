package percolation;

import edu.princeton.cs.algs4.WeightedQuickUnionUF;

/**
 * 
 * @author pankaj
 *
 */
public class Percolation {
	private int[][] grid;
	private WeightedQuickUnionUF wquuf = null;
	private int N;
	private int TOP = 0;
	private int BOTTOM = 0;

	public Percolation(int N) {
		checkArguments(N);
		this.N = N;
		// create N-by-N grid, with all sites blocked
		grid = new int[N][N];
		wquuf = new WeightedQuickUnionUF((N * N) + 1);
		BOTTOM = N * N;
	}

	private void checkArguments(int n2) {
		if (n2 <= 0) {
			throw new IllegalArgumentException();
		}
	}

	// open site (row i, column j) if it is not open already
	public void open(int x, int y) {
		if (isOpen(x, y))
			return;
		grid[x - 1][y - 1] = -1;

		if ((x - 1) == 0) {
			wquuf.union(TOP, convert2Dinto1D(x - 1, y - 1));
		} else if ((x - 1) == (N - 1)) {
			wquuf.union(BOTTOM, convert2Dinto1D(x - 1, y - 1));
		}

		if (N == 1) {
			wquuf.union(TOP, BOTTOM);
		}

		int[] neighbours = getNeighbours(x - 1, y - 1);

		for ( int i = 0; i < neighbours.length; i = i + 2 ) {
			if (isValid(neighbours, i) && isValid(neighbours, i + 1)) {
				if (isOpenInternal(neighbours[i], neighbours[i + 1])) {
					int xAx = convert2Dinto1D((x - 1), (y - 1));
					int yAx = convert2Dinto1D(neighbours[i], neighbours[i + 1]);
					if (!wquuf.connected(xAx, yAx))
						wquuf.union(xAx, yAx);
				}
			}
		}
	}

	private boolean isValid(int[] neighbours, int i) {
		return (neighbours[i] >= 0 && neighbours[i] < N);
	}

	// is site (row i, column j) open?
	public boolean isOpen(int i, int j) {
		return isOpenInternal(i - 1, j - 1);
	}

	private boolean isOpenInternal(int i, int j) {
		return (grid[i][j] == -1);
	}

	// is site (row i, column j) full?
	public boolean isFull(int i, int j) {
		// if ( grid[i - 1][j - 1] == -999 ) {
		// return true;
		// }
		return isConnectedToTheTop(i - 1, j - 1);
	}

	private boolean isConnectedToTheTop(int row, int col) {
		if (!isOpenInternal(row, col))
			return false;
		//
		// if ( row == 0 ) {
		// return true;
		// }
		// for ( int k = 0; k < N; k++ ) {
		// if ( wquuf.connected(convert2Dinto1D(0, k), convert2Dinto1D(row, col)) ) {
		// return true;
		// }
		// }
		// return false;
		return wquuf.connected(TOP, convert2Dinto1D(row, col));
	}

	// does the system percolate?
	public boolean percolates() {
		// check if any of the bottom cells are connected to the top
		// for ( int i = 0; i < N; i++ ) {
		// if ( isFull(N, i + 1) ) {
		// return true;
		// }
		// }
		return (wquuf.connected(TOP, BOTTOM));
		// return false;
	}

	private int convert2Dinto1D(int row, int col) {
		int res = 0;
		int tempRow = row;
		while (tempRow != 0) {
			res = res + N;
			tempRow-- ;
		}
		return res + col;
	}

	private int[] getNeighbours(int i, int j) {
		return new int[] { i - 1, j, i + 1, j, i, j - 1, i, j + 1 };
	}

	public static void main(String[] args) {
		Percolation percolation = new Percolation(200);
		percolation.percolates();
	}
}