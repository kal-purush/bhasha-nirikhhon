package com.jeff.alphamao;

/**
 * Created by jeff on 17-5-30.
 */

public class PieceInformation {
    private int index;
    private int stepToEnd;
    private boolean canWin;

    public PieceInformation(int index, int stepToEnd, boolean canWin) {
        this.index = index;
        this.stepToEnd = stepToEnd;
        this.canWin = canWin;
    }

    public void addStepToEnd() { ++stepToEnd; }
    public void setCanWin(boolean canWin) { this.canWin = canWin; }

    public int getStepToEnd() { return stepToEnd; }
    public boolean getCanWin() { return canWin; }
}