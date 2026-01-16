package labyrint;

import java.util.Random;

public class Lab implements Labyrinth {

    private int height;
    private int width;

    private boolean[][] wallRight;
    private boolean[][] wallBelow;
    private int[][] annotations;

    private final int NONE = 0;
    private final int LEFT = 1;
    private final int RIGHT = 2;
    private final int UP = 3;
    private final int DOWN = 4;
    private final int HERE = 5;

    private Random t = new Random();
    private int arrow = 0;

    public Lab(int width, int height) {
        this.width = width;
        this.height = height;
        wallRight = new boolean[width][height];
        wallBelow = new boolean[width][height];
        annotations = new int[width][height];
        for (int x=0;x<width;x++) {
            for (int y=0;y<height;y++) {
                wallRight[x][y] = false;
                wallBelow[x][y] = false;
                annotations[x][y] = NONE;
            }
        }
        placeAllArrow();
    }

    private int intOfDir(Direction d) {
        if (d == Labyrinth.Direction.LEFT) { return LEFT; }
        else if (d == Labyrinth.Direction.RIGHT) { return RIGHT; }
        else if (d == Labyrinth.Direction.UP) { return UP; }
        else { return DOWN; }
    }

    public void setMark(int x,int y,boolean b) {
        if (x < 0 || width <= x) { return; }
        if (y < 0 || height <= y) { return; }
        if (b) {
            annotations[x][y] = HERE;
        } else {
            annotations[x][y] = NONE;
        }
    }

    public boolean getMark(int x,int y) {
        if (x < 0 || width <= x) { return false; }
        if (y < 0 || height <= y) { return false; }
        return (annotations[x][y] != NONE);
    }

    public boolean canMove(Direction dir, int x, int y) {
        if (x < 0 || width <= x) { return false; }
        if (y < 0 || height <= y) { return false; }
        if (dir == Labyrinth.Direction.LEFT) {
            if (x <= 0) { return false; }
            return !(wallRight[x-1][y]);
        } else if (dir == Labyrinth.Direction.RIGHT) {
            if (width-1 <= x) { return false; }
            return !(wallRight[x][y]);
        } else if (dir == Labyrinth.Direction.UP) {
            if (y <= 0) { return false; }
            return !(wallBelow[x][y-1]);
        } else if (dir == Labyrinth.Direction.DOWN) {
            if (height-1 <= y) { return false; }
            return !(wallBelow[x][y]);
        } else {
            return false;
        }
    }

    public String toString() {
        String str = "";
        for (int y=0;y<height;y++) {
            for (int x=0;x<width;x++) {
                if (canMove(Labyrinth.Direction.UP,x,y)) {
                    str = str + "+  ";
                } else {
                    str = str + "+--";
                }
            }
            str = str + "+\n";
            for (int x=0;x<width;x++) {
                if (canMove(Labyrinth.Direction.LEFT,x,y)) {
                    str = str + " ";
                } else {
                    str = str + "|";
                }
                if (annotations[x][y] == RIGHT) {
                    str = str + "->";
                } else if (annotations[x][y] == LEFT) {
                    str = str + "<-";
                } else if (annotations[x][y] == UP) {
                    str = str + "↑↑";
                } else if (annotations[x][y] == DOWN) {
                    str = str + "↓↓";
                } else if (annotations[x][y] == HERE) {
                    str = str + "XX";
                } else {
                    str = str + "  ";
                }
            }
            str = str + "|\n";
        }
        for (int x=0;x<width;x++) {
            str = str + "+--";
        }
        str = str + "+\n";
        return str;
    }

    private void placeArrow(int x, int y) {
        int k = t.nextInt(4);
        if (annotations[x][y] == NONE) {
            if (k == 0 && x < width-1) {
                if (annotations[x+1][y] != NONE) {
                    annotations[x][y] = RIGHT;
                    arrow++;
                }
            }
            if (k == 1 && 0 < x) {
                if (annotations[x-1][y] != NONE) {
                    annotations[x][y] = LEFT;
                    arrow++;
                }
            }
            if (k == 2 && y < height-1) {
                if (annotations[x][y+1] != NONE) {
                    annotations[x][y] = DOWN;
                    arrow++;
                }
            }
            if (k == 3 && 0 < y) {
                if (annotations[x][y-1] != NONE) {
                    annotations[x][y] = UP;
                    arrow++;
                }
            }
        }
    }

    private void placeAllArrow() {
        arrow = 1;
        annotations[0][0] = HERE;
        while (arrow < height * width) {
            for (int x=0;x<width;x++) {
                for (int y=0;y<height;y++) {
                    placeArrow(x,y);
                }
            }
        }
        for (int x=0;x<width-1;x++) {
            for (int y=0;y<height;y++) {
                wallRight[x][y] = (annotations[x][y] != RIGHT) &&
                    (annotations[x+1][y] != LEFT);
            }
        }
        for (int x=0;x<width;x++) {
            for (int y=0;y<height-1;y++) {
                wallBelow[x][y] = (annotations[x][y] != DOWN) &&
                    (annotations[x][y+1] != UP);
            }
        }
        for (int x=0;x<width;x++) {
            for (int y=0;y<height;y++) {
                annotations[x][y] = NONE;
            }
        }
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

}