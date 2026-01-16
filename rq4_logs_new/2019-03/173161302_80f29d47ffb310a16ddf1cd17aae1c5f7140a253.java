package lesson1;

public class Dog extends Animal implements Swim, Jump{
    private final int canSwimDistance;
    private int canJumpHeight;

    public Dog(String name, int canRunDistance, int canSwimDistance, int canJumpHeight) {
        super(name, canRunDistance);
        this.canSwimDistance = canSwimDistance;
        this.canJumpHeight = canJumpHeight;
    }

    @Override
    public void jump(int height) {
        if (this.canJumpHeight < height) {
            setOnDistance(false);
        }

    }

    @Override
    public void swim(int distance) {
        if (this.canSwimDistance < distance) {
            setOnDistance(false);
        }

    }
}