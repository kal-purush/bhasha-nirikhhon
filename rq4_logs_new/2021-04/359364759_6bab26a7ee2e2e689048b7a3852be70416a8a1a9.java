package com.github.arsengir;

public class Frog {
    public static final int MIN_POSITION = 0;
    public static final int MAX_POSITION = 10;

    protected int position;

    public Frog() {
        position = 5;
    }

    public boolean jump(int steps) {
        int positionNext = position + steps;
        if (positionNext > MAX_POSITION || positionNext < MIN_POSITION) {
            return false;
        }
        position = positionNext;
        return true;
    }

}