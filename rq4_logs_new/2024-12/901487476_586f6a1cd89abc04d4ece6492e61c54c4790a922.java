import java.util.*;
import java.awt.Point;


public class Antenna {
    private char frequency;
    private Point pos;

    public Antenna(char freq, Point pos) {
        this.frequency = freq;
        this.pos = pos;
    }

    public char getFrequency() {
        return this.frequency;
    }

    public Point getPosition() {
        return this.pos;
    }

    @Override
    public String toString() {
        return "(" + pos.x + ", " + pos.y + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Antenna other = (Antenna) obj;
        return other.getPosition() == this.getPosition() && other.getFrequency() == this.getFrequency();
    }
}