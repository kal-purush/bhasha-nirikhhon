package com.bosch.robot;

public enum Orientation {
    EAST, SOUTH, WEST, NORTH;
        
    Orientation turnTo(boolean left) {
        return left ? toLeft() : toRight();
    }

    Point moveTo(Point point, int step) {
        return point.move(xOffset(step), yOffset(step));
    }
    
    private int xOffset(int step) {
        return step * OFFSETS[ordinal()];
    }
    
    private int yOffset(int step) {
        return step * OFFSETS[3 - ordinal()];
    }
    
    private Orientation toLeft() {
        return values()[(ordinal() + 3) % 4];
    }
    
    private Orientation toRight() {
        return values()[(ordinal() + 1) % 4];
    }
    
    private static final int[] OFFSETS = {1, 0, -1, 0};
}