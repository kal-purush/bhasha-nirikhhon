package Transport;

import java.util.concurrent.ThreadLocalRandom;

public class Buses extends Transport implements Competing{


    public Buses(String brand, String model, float engineVolume) {
        super(brand, model, engineVolume);
    }
    @Override
    public double getBestLapTime() {
        return ThreadLocalRandom.current().nextDouble(0.00,999.99);
    }

    @Override
    public int getMaxSpeed() {
        return ThreadLocalRandom.current().nextInt(1,150);
    }
    @Override
    public void startMoving() {
        System.out.println("Bus, " + getBrand() + " " + getModel() + ", start moving!");
    }

    @Override
    public void endMoving() {
        System.out.println("Bus, " + getBrand() + " " + getModel() + ", end moving!");
    }

    @Override
    public void pitStop() {
        System.out.println("Bus, " + getBrand() + " " + getModel() + ", went to the pit-stop!");
    }
    @Override
    public void circleIndicators() {
        System.out.println("Bus, " + getBrand() + " " + getModel() +
                ". Lap time - " + getBestLapTime() + ". Max speed - " + getMaxSpeed() + ".");
    }


}