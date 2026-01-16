package java_final;

public abstract class Auto {
    private String name;
    private String color;
    private Fuel fuel;

    public abstract void autoInfo();

    public String getName() {
        return name;
    }

    public String getColor() {
        return color;
    }

    public Fuel getFuel() {
        return fuel;
    }

}