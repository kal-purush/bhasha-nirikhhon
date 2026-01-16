package uwaterloo.ca.lab4_203_06;

/**
 * Created by Cristiano Chelotti on 7/9/2017.
 */

public class Position {
    float xPos;
    float yPos;
    boolean occupied;

    public Position(float x, float y, boolean occupied) {
        xPos = x;
        yPos = y;
        this.occupied = occupied;
    }

    public float getX() {
        return xPos;
    }

    public float getY() {
        return yPos;
    }

    public boolean isOccupied() {
        return occupied;
    }

    public void setOccupied(boolean occupied) {
        this.occupied = occupied;
    }

}