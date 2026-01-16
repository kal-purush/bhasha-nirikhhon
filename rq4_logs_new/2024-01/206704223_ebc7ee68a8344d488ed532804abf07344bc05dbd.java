package com.coding.challenge.dp;

import java.util.Arrays;

class ZeroOneMatrix {
	ZeroOneMatrix() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] mat1 = { { 0, 0, 0 }, { 0, 1, 0 }, { 0, 0, 0 } };
		final int[][] d1 = { { 0, 0, 0 }, { 0, 1, 0 }, { 0, 0, 0 } };
		assert Arrays.deepEquals(d1, updateMatrix(mat1));

		final int[][] mat2 = { { 0, 0, 0 }, { 0, 1, 0 }, { 1, 1, 1 } };
		final int[][] d2 = { { 0, 0, 0 }, { 0, 1, 0 }, { 1, 2, 1 } };
		assert Arrays.deepEquals(d2, updateMatrix(mat2));

		final int[][] mat3 = { { 0, 1, 1, 0, 1, 0, 1, 0, 1, 0 }, { 1, 1, 1, 0, 0, 0, 1, 0, 0, 1 },
				{ 0, 1, 1, 1, 0, 1, 1, 0, 1, 1 }, { 1, 1, 1, 1, 0, 1, 1, 1, 1, 0 }, { 1, 0, 1, 1, 1, 1, 1, 1, 1, 1 },
				{ 1, 1, 1, 1, 0, 0, 1, 0, 1, 1 }, { 1, 1, 0, 1, 0, 0, 1, 1, 1, 1 }, { 1, 1, 0, 1, 0, 0, 1, 0, 0, 0 },
				{ 0, 0, 1, 0, 1, 0, 1, 1, 1, 0 }, { 1, 1, 1, 1, 0, 1, 1, 0, 1, 1 } };
		final int[][] d3 = { { 0, 1, 1, 0, 1, 0, 1, 0, 1, 0 }, { 1, 2, 1, 0, 0, 0, 1, 0, 0, 1 },
				{ 0, 1, 2, 1, 0, 1, 1, 0, 1, 1 }, { 1, 1, 2, 1, 0, 1, 2, 1, 1, 0 }, { 1, 0, 1, 2, 1, 1, 2, 1, 2, 1 },
				{ 2, 1, 1, 1, 0, 0, 1, 0, 1, 2 }, { 2, 1, 0, 1, 0, 0, 1, 1, 1, 1 }, { 1, 1, 0, 1, 0, 0, 1, 0, 0, 0 },
				{ 0, 0, 1, 0, 1, 0, 1, 1, 1, 0 }, { 1, 1, 2, 1, 0, 1, 1, 0, 1, 1 } };
		assert Arrays.deepEquals(d3, updateMatrix(mat3));
	}

	static int[][] updateMatrix(int[][] mat) {
		final int m = mat.length;
		final int n = mat[0].length;

		final int[][] topLeft = new int[m][n];
		if (mat[0][0] == 1)
			topLeft[0][0] = 10001;
		// start from top-left and solve.
		for (int j = 1; j < n; j++)
			if (mat[0][j] == 1)
				topLeft[0][j] = topLeft[0][j - 1] + 1;

		for (int i = 1; i < m; i++)
			if (mat[i][0] == 1)
				topLeft[i][0] = topLeft[i - 1][0] + 1;

		for (int i = 1; i < m; i++)
			for (int j = 1; j < n; j++)
				if (mat[i][j] == 1)
					topLeft[i][j] = Math.min(topLeft[i - 1][j], topLeft[i][j - 1]) + 1;

		// start from the top-right and solve.
		final int[][] topRight = new int[m][n];
		if (mat[0][n - 1] == 1)
			topRight[0][n - 1] = 10001;
		for (int j = n - 2; j >= 0; j--)
			if (mat[0][j] == 1)
				topRight[0][j] = topRight[0][j + 1] + 1;

		for (int i = 1; i < m; i++)
			if (mat[i][n - 1] == 1)
				topRight[i][n - 1] = topRight[i - 1][n - 1] + 1;

		for (int i = 1; i < m; i++)
			for (int j = n - 2; j >= 0; j--)
				if (mat[i][j] == 1)
					topRight[i][j] = Math.min(topRight[i - 1][j], topRight[i][j + 1]) + 1;

		// start from the bottom left.
		final int[][] bottomLeft = new int[m][n];
		if (mat[m - 1][0] == 1)
			bottomLeft[m - 1][0] = 10001;
		for (int i = m - 2; i >= 0; i--)
			if (mat[i][0] == 1)
				bottomLeft[i][0] = bottomLeft[i + 1][0] + 1;

		for (int j = 1; j < n; j++)
			if (mat[m - 1][j] == 1)
				bottomLeft[m - 1][j] = bottomLeft[m - 1][j - 1] + 1;

		for (int i = m - 2; i >= 0; i--)
			for (int j = 1; j < n; j++)
				if (mat[i][j] == 1)
					bottomLeft[i][j] = Math.min(bottomLeft[i + 1][j], bottomLeft[i][j - 1]) + 1;

		// start from the bottom right.
		final int[][] bottomRight = new int[m][n];
		if (mat[m - 1][n - 1] == 1)
			bottomRight[m - 1][n - 1] = 10001;
		for (int i = m - 2; i >= 0; i--)
			if (mat[i][n - 1] == 1)
				bottomRight[i][n - 1] = bottomRight[i + 1][n - 1] + 1;

		for (int j = n - 2; j >= 0; j--)
			if (mat[m - 1][j] == 1)
				bottomRight[m - 1][j] = bottomRight[m - 1][j + 1] + 1;

		for (int i = m - 2; i >= 0; i--)
			for (int j = n - 2; j >= 0; j--)
				if (mat[i][j] == 1)
					bottomRight[i][j] = Math.min(bottomRight[i][j + 1], bottomRight[i + 1][j]) + 1;

		// computing the min distance overall.
		final int[][] distance = new int[m][n];
		for (int i = 0; i < m; i++)
			for (int j = 0; j < n; j++)
				if (mat[i][j] == 1)
					distance[i][j] = Math.min(Math.min(topLeft[i][j], topRight[i][j]),
							Math.min(bottomLeft[i][j], bottomRight[i][j]));

		return distance;
	}
}