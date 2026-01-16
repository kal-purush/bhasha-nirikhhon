package com.company;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class MainWithoutDeadLock {
    public static void main(String[] args) {
        ArrayList<SmartPhilosopher> philophers = new ArrayList<SmartPhilosopher>();
        ArrayList<Integer> requesters = new ArrayList<>();
        int[] forks = new int[5];
        ArrayList<ReentrantLock> locks = new ArrayList<>();

        for (int i = 0; i < forks.length; i++) {
            forks[i] = 0; //assign that all forks are down.
        }

        for (int i = 0; i < forks.length; i++) {
            locks.add(new ReentrantLock());
        }

        for (int i = 0; i < 5; i++) {
            philophers.add(new SmartPhilosopher(i, forks, locks));
        }

        for (int i = 0; i < philophers.size(); i++) {
            philophers.get(i).start();
        }
    }
}