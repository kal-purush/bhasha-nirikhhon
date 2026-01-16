package adventofcode.day3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Claim {
    private int id;
    private int x;
    private int y;
    private int width;
    private int height;
    private static Pattern claimPattern = Pattern.compile("#(\\d+) @ (\\d+),(\\d+): (\\d+)x(\\d+)");

    public Claim(String input) {
        Matcher matcher = claimPattern.matcher(input);

        if (matcher.find()) {
            this.id = Integer.valueOf(matcher.group(1));
            this.x = Integer.valueOf(matcher.group(2));
            this.y = Integer.valueOf(matcher.group(3));
            this.width = Integer.valueOf(matcher.group(4));
            this.height = Integer.valueOf(matcher.group(5));
        }
    }

    public boolean overlaps(Claim other) {
        return (this.getX() < other.getX()+other.getWidth() &&
                this.getX()+this.getWidth() > other.getX() &&
                this.getY() < other.getY()+other.getHeight() &&
                this.getY()+this.getHeight() > other.getY());
    }

    public int getId() {
        return id;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }
}