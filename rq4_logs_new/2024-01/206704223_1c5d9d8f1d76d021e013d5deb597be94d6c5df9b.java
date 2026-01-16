package com.coding.challenge.simulation;

class AverageWaitingTime {
	AverageWaitingTime() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] customers1 = { { 1, 2 }, { 2, 5 }, { 4, 3 } };
		assert averageWaitingTime(customers1) == 5.0;

		final int[][] customers2 = { { 5, 2 }, { 5, 4 }, { 10, 3 }, { 20, 1 } };
		assert averageWaitingTime(customers2) == 3.25;
	}

	static double averageWaitingTime(int[][] customers) {
		final int n = customers.length;
		long totalWaitingTime = 0;
		for (int i = 0, t = customers[0][0]; i < n; i++) {
			t = Math.max(t, customers[i][0]) + customers[i][1];
			totalWaitingTime = totalWaitingTime + t - customers[i][0];
		}
		return totalWaitingTime * 1.0 / n;
	}
}