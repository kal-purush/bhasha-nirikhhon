package O5_practice.exercise3_composition_inheritance;

public class LineSub extends Point{

    Point end;
    public LineSub(int beginX, int beginY, int endX, int endY) {
        super(beginX, beginY);
        end = new Point(endX, endY);
    }

    public LineSub(Point begin, Point end) {
        super(begin.getX(), begin.getY());
        this.end = end;
    }

    public String toString() {
        return "LineSub: (" + this.getX() + ", " + this.getY() + ") to (" + this.end.getX() + ", " + this.end.getY() + ")";
    }

    public Point getBegin(){
        return new Point(this.getX(), this.getY());
    }

    public Point getEnd() {
        return end;
    }

    public void setBegin(Point begin) {
        this.setXY(begin.getX(),begin.getY());
    }

    public void setEnd(Point end) {
        this.end = end;
    }

    public int getBeginX() {
        return this.getX();
    }

    public int getBeginY() {
        return this.getY();
    }

    public int getEndX() {
        return this.end.getX();
    }

    public int getEndY() {
        return this.end.getY();
    }

    public void setBeginX(int x) {
        this.setX(x);
    }

    public void setBeginY(int y) {
        this.setY(y);
    }

    public void setBeginXY(int x, int y) {
        this.setXY(x,y);
    }

    public void setEndX(int x) {
        this.end.setX(x);
    }

    public void setEndY(int y) {
        this.end.setY(y);
    }

    public void setEndXY(int x, int y) {
        this.end.setXY(x,y);
    }

    public int getLength() {
        int xDiff = Math.abs(this.getX() - this.end.getX());
        int yDiff = Math.abs(this.getY() - this.end.getY());
        return (int) Math.sqrt((xDiff * xDiff) + (yDiff * yDiff));
    }

    public double getGradient() {
        int xDiff = Math.abs(this.getX() - this.end.getX());
        int yDiff = Math.abs(this.getY() - this.end.getY());
        return Math.atan2(yDiff, xDiff);
    }

}