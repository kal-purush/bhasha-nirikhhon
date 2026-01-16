package chapter9.exercise08;

public class Point implements Comparable<Point> {
    private int x;
    private int y;

    Point(int x, int y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public int compareTo(Point other) {
        if (this == other) return 0;
        if (other == null) {
            throw new NullPointerException("other must not be null");
        }

        if (x > other.x) {
            return 1;
        } else if (x < other.x) {
            return -1;
        }

        if (y > other.y) {
            return 1;
        } else if (y < other.y) {
            return -1;
        }
        return 0;
    }
}