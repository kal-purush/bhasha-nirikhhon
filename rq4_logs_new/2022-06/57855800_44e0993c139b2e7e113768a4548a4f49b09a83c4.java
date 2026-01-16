package com.github.lucaseo90.medium;

import java.util.PriorityQueue;

public class No0973 {
    public int[][] kClosest(int[][] points, int k) {
        PriorityQueue<Point> priorityQueue = new PriorityQueue();
        for (int i = 0; i < points.length; i++) {
            int x = points[i][0];
            int y = points[i][1];
            double distance = Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2));
            priorityQueue.add(new Point(distance, x, y));
        }

        int[][] answers = new int[k][];
        for (int i = 0; i < k; i++) {
            Point point = priorityQueue.poll();

            answers[i] = new int[2];
            answers[i][0] = point.x;
            answers[i][1] = point.y;
        }

        return answers;

    }

    private class Point implements Comparable<Point> {
        double distance;
        int x;
        int y;

        public Point(double distance, int x, int y) {
            this.distance = distance;
            this.x = x;
            this.y = y;
        }

        @Override
        public int compareTo(Point o) {
            return this.distance <= o.distance ? -1 : 1;
        }
    }
}