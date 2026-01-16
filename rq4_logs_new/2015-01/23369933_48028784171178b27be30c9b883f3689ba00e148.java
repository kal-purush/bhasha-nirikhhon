package chapter9.exercise09;

import java.util.Objects;

public class LabeledPoint {
    private String label;
    private int x;
    private int y;

    public LabeledPoint(String label, int x, int y) {
        this.label = label;
        this.x = x;
        this.y = y;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if ( !(otherObject instanceof LabeledPoint) ) {
            return false;
        }
        LabeledPoint other = (LabeledPoint) otherObject;

        return Objects.equals(label, other.label) &&
                x == other.x && y == other.y;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + x;
        result = 31 * result + y;
        result = 31 * Objects.hash(label);
        return result;
    }
}