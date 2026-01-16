package box0429;

public class Box {
    int length;
    int width;
    int height;

    public Box(int length, int width, int height) {
        this.length = length;
        this.width = width;
        this.height = height;
    }

    public boolean isFit(int length, int width, int height) {
        if (length <= this.length && width <= this.width && height <= this.height) {
            return true;
        } else {
            return false;
        }
    }
}