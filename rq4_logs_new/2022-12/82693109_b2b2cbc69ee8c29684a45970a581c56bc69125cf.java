package leetcode.p1834;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

// Single-Threaded CPU
// The CPU can finish a task then start a new one instantly
class Solution {
    public int[] getOrder(int[][] tasks) {
        final int n = tasks.length;
        final Task[] taskArr = new Task[n];
        for (int i = 0; i < n; i++) {
            taskArr[i] = new Task(i, tasks[i][0], tasks[i][1]);
        }
        // sort by enqueued time first
        Arrays.sort(taskArr, Comparator.comparing(t -> t.enqueuedTime));

        // those conditions will be sorted in the offering process
        final PriorityQueue<Task> queue = new PriorityQueue<>((t1, t2) -> {
            if (t1.processingTime.equals(t2.processingTime)) {
                return t1.index.compareTo(t2.index);
            }
            return t1.processingTime.compareTo(t2.processingTime);
        });

        final int[] order = new int[n];
        int i = 0; // index of taskArr
        int ord = 0; // index of order
        int currentTime = 0;
        while (ord < n) {
            while (i < n && taskArr[i].enqueuedTime <= currentTime) {
                // can offer task only when there isn't any processing task
                queue.offer(taskArr[i++]);
            }
            if (queue.isEmpty()) {
                // an initial time set
                // or
                // a time set after an idle
                currentTime = taskArr[i].enqueuedTime;
            } else {
                // consume the task once (constrained as a single-threaded CPU)
                currentTime += queue.peek().processingTime;
                order[ord++] = queue.poll().index;
            }
        }
        // an order of dequeue
        return order;
    }
}

// sort by
// 1. enqueued time
// 2. the shortest processing time
// 3. the smallest index
class Task {
    Integer index;
    Integer enqueuedTime;
    Integer processingTime;

    Task(int index, int enqueuedTime, int processingTime) {
        this.index = index;
        this.enqueuedTime = enqueuedTime;
        this.processingTime = processingTime;
    }
}