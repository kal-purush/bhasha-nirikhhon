package lesson1;

public class Animal implements Run {
    private String name;
    private boolean onDistance = true;
    private int canRunDistance;

    public Animal(String name, int canRunDistance) {
        this.name = name;
        this.canRunDistance = canRunDistance;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void run(int distance) {
        if (this.canRunDistance < distance) {
            this.onDistance = false;
        }
    }

    public boolean isOnDistance () {
        return onDistance;
    }

    public Animal setOnDistance (boolean onDistance) {
        this.onDistance = onDistance;
        return this;
    }
}