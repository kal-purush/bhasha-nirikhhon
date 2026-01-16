package practise;

public class Ferrari {
    private double price;
    private int maxSpeed;

    public Ferrari(double price, int maxSpeed) {
        this.price = price;
        this.maxSpeed = maxSpeed;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public void setMaxSpeed(int maxSpeed) {
        this.maxSpeed = maxSpeed;
    }

    public double getPrice() {
        return this.price;
    }

    public int getMaxSpeed() {
        return this.maxSpeed;
    }

    @Override
    public String toString() {
        return this.price + " " + this.maxSpeed;
    }
}