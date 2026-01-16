package com.concurrent.thread;

import java.util.Iterator;
import java.util.List;


public class ThreadPoolStats implements Iterable<ThreadPoolStats.Stats> {

    public static class Stats {

        private String name;
        private int threads;
        private int queue;
        private int active;
        private long rejected;
        private int largest;
        private long completed;

        Stats() {

        }

        public Stats(String name, int threads, int queue, int active, long rejected, int largest, long completed) {
            this.name = name;
            this.threads = threads;
            this.queue = queue;
            this.active = active;
            this.rejected = rejected;
            this.largest = largest;
            this.completed = completed;
        }

        public String getName() {
            return this.name;
        }

        public int getThreads() {
            return this.threads;
        }

        public int getQueue() {
            return this.queue;
        }

        public int getActive() {
            return this.active;
        }

        public long getRejected() {
            return rejected;
        }

        public int getLargest() {
            return largest;
        }

        public long getCompleted() {
            return this.completed;
        }

    }

    private List<Stats> stats;

    ThreadPoolStats() {

    }

    public ThreadPoolStats(List<Stats> stats) {
        this.stats = stats;
    }

    @Override
    public Iterator<Stats> iterator() {
        return stats.iterator();
    }

}