package by.teachmeskills.classesAndObjects.computer;

public class Computer {
    private final double price;
    private final String model;
    private final Ram ram;
    private final Hdd hdd;

    public Computer(double price, String model) {
        this.price = price;
        this.model = model;
        this.ram = new Ram();
        this.hdd = new Hdd();
    }

    public Computer(double price, String model, Ram ram, Hdd hdd) {
        this.price = price;
        this.model = model;
        this.ram = ram;
        this.hdd = hdd;
    }

    @Override
    public String toString() {
        return "Computer parameters are {" +
                "price=" + price +
                ", model='" + model + '\'' +
                ", " + ram +
                ", " + hdd +
                '}';
    }
}