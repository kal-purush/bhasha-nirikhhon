package br.dev.ferreiras.challenges.leetCode;

import java.util.Arrays;

/*
You are given an array of CPU tasks, each represented by letters A to Z, and a cooling time, n.
Each cycle or interval allows the completion of one task.
Tasks can be completed in any order, but there's a constraint:
identical tasks must be separated by at least n intervals due to cooling time.

Return the minimum number of intervals required to complete all tasks.

Example 1:

Input: tasks = ["A","A","A","B","B","B"], n = 2

Output: 8

Explanation:
A possible sequence is: A -> B -> idle -> A -> B -> idle -> A -> B.

After completing task A, you must wait two cycles before doing A again.
The same applies to task B. In the 3rd interval, neither A nor B can be done, so you idle.
By the 4th cycle, you can do A again as 2 intervals have passed.

Example 2:

Input: tasks = ["A","C","A","B","D","B"], n = 1

Output: 6

Explanation:
A possible sequence is: A -> B -> C -> D -> A -> B.

With a cooling interval of 1, you can repeat a task after just one other task.

Example 3:

Input: tasks = ["A","A","A", "B","B","B"], n = 3

Output: 10

Explanation:
A possible sequence is: A -> B -> idle -> idle -> A -> B -> idle -> idle -> A -> B.

There are only two types of tasks, A and B, which need to be separated by 3 intervals.
This leads to idling twice between repetitions of these tasks

Please see...
https://medium.com/@satyem77/task-scheduler-leetcode-39d579f3440
 max ( (n+1)*(f-1) + X, TotalJobs)
 */
public class TaskScheduler {

  public static void main(String[] args) {
    char[] tasks = {'A', 'A', 'A', 'B', 'B', 'B'};
    int n = 2;

    int r = TaskScheduler.leastInterval(tasks, n);
    System.out.println(r);

  }

  public static int leastInterval(char[] tasks, int n) {
    int[] frequency = new int[26];
    int maxFrequency = 0;
    int totalTasks = tasks.length;

    for (char task : tasks) {
      frequency[task - 'A']++;
    }

    Arrays.sort(frequency);
    maxFrequency = frequency[25] - 1;
    int slots = maxFrequency * n;

    for (int i = 24; i >= 0; i--) {
      slots = slots - Math.min(maxFrequency, frequency[i]);
    }

    return slots > 0 ? totalTasks + slots : totalTasks;
  }
}