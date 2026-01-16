package com.coding.challenge.heap;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

class SingleThreadedCPU {
	SingleThreadedCPU() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] tasks1 = { { 1, 2 }, { 2, 4 }, { 3, 2 }, { 4, 1 } };
		final int[] a1 = { 0, 2, 3, 1 };
		assert Arrays.equals(a1, getOrder(tasks1));

		final int[][] tasks2 = { { 7, 10 }, { 7, 12 }, { 7, 5 }, { 7, 4 }, { 7, 2 } };
		final int[] a2 = { 4, 3, 2, 0, 1 };
		assert Arrays.equals(a2, getOrder(tasks2));

		final int[][] tasks3 = { { 5, 2 }, { 7, 2 }, { 9, 4 }, { 6, 3 }, { 5, 10 }, { 1, 1 } };
		final int[] a3 = { 5, 0, 1, 3, 2, 4 };
		assert Arrays.equals(a3, getOrder(tasks3));
	}

	static int[] getOrder(int[][] tasks) {
		final int n = tasks.length;
		// positioned tasks.
		final int[][] positionedTasks = new int[n][3];
		for (int i = 0; i < n; i++)
			positionedTasks[i] = new int[] { tasks[i][0], tasks[i][1], i };
		// assuming a stable sort.
		final Comparator<int[]> fifoCmp = new Comparator<int[]>() {
			@Override
			public int compare(int[] o1, int[] o2) {
				// break the tie using the task processing time.
				if (o1[0] == o2[0])
					return Integer.compare(o1[1], o2[1]);
				return Integer.compare(o1[0], o2[0]);
			}
		};
		Arrays.sort(positionedTasks, fifoCmp);

		final Comparator<int[]> shortestTaskFirstCmp = new Comparator<int[]>() {
			@Override
			public int compare(int[] o1, int[] o2) {
				if (o1[1] == o2[1])
					return Integer.compare(o1[2], o2[2]);
				return Integer.compare(o1[1], o2[1]);
			}
		};
		final Queue<int[]> queuedTasks = new PriorityQueue<>(shortestTaskFirstCmp);
		queuedTasks.add(positionedTasks[0]);
		int i = 0;
		int j = 1;
		final int[] a = new int[n];
		int time = positionedTasks[0][0];
		while (!queuedTasks.isEmpty()) {
			final int[] task = queuedTasks.poll();
			a[i] = task[2];
			i = i + 1;
			time = time + task[1];
			// add all the other tasks arrived at the queue while processing this task.
			while (j < n && positionedTasks[j][0] <= time) {
				queuedTasks.offer(positionedTasks[j]);
				j = j + 1;
			}
			if (queuedTasks.isEmpty() && j < n) {
				queuedTasks.offer(positionedTasks[j]);
				// start over again.
				time = positionedTasks[j][0];
				j = j + 1;
			}
		}
		return a;
	}
}