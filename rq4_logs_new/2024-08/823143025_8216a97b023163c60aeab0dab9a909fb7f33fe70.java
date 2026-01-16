package transport;

public class DriveB extends Driver{
    public DriveB(String name, boolean hasDriverLicense, int experienceInYear) {
        super(name, hasDriverLicense, experienceInYear);
    }

    @Override
    public void startMove() {
        System.out.println("Водитель категории В " + getName() + " начал движение");
    }

    @Override
    public void finishMove() {
        System.out.println("Водитель категории В " + getName() + " закончил движение");
    }

    @Override
    public void refill() {
        System.out.println("Водитель категории В " + getName() + " заправляет авто");
    }
}