package algorithm.tuan5;

import java.util.ArrayList;

import algorithm.tuan1.Graph;

public class Floyd {

	private Graph matrix;
	int[][] index;

	public Floyd(Graph g) {
		matrix = g;
	}

	public ArrayList<ArrayList<Integer>> shortestPath() {
		ArrayList<ArrayList<Integer>> mt = new ArrayList<ArrayList<Integer>>(matrix.getMatrix());
		int n = matrix.getMatrix().size();
		index = new int[n][n];
		for (int i = 0; i < n; n++) {
			for (int j = 0; j < n; j++) {
				if (mt.get(i).get(j) != 0) {
					index[i][j] = j;
				}
			}
		}
		for (int k = 0; k < n; k++) {
			for (int i = 0; i < n; i++) {
				for (int j = 0; j < n; j++) {
					if (mt.get(i).get(j) > mt.get(i).get(k) + mt.get(k).get(j)) {
						mt.get(i).set(j, mt.get(i).get(k) + mt.get(k).get(j));
						index[i][j] = index[i][k];
					}
				}
			}
		}
		return mt;
	}

	public String shortestPathExtend(int a, int b) {
		shortestPath();
		ArrayList<Integer> result = new ArrayList<Integer>();
		result.add(a);
		int dest = -1;
		while (dest != b) {
			dest = index[dest][b];
			result.add(dest);
		}
		String r = "";
		for (int i = 0; i < result.size() - 1; i++) {
			r += result.get(i) + "=>";
		}
		r += result.get(result.size() - 1);
		return r;
	}

	public static void main(String[] args) {
		Graph matrix = new Graph();
		matrix.addPoint();
		matrix.addPoint();
		matrix.addPoint();
		matrix.addPoint();
		matrix.addPoint();
		matrix.addPoint();
		matrix.addPoint();
		matrix.addPoint();
		matrix.addPoint();
		Floyd p = new Floyd(matrix);

	}
}