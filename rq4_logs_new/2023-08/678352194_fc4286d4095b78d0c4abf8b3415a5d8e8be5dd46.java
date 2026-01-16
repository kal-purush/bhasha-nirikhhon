package homeworks.homework3;

import java.awt.*;

public abstract class Car {

    private String brand;
    private String model;
    private Color color;
    private TypeBodyCar bodyType;
    private int numberWheels;
    private TypeFuel fuelType;
    private TypeGearbox gearbox;
    private double valueEngine;

    public Car(String brand, String model, Color color, TypeBodyCar bodyType, int numberWheels, TypeFuel fuelType, TypeGearbox gearbox, double valueEngine) {
        this.brand = brand;
        this.model = model;
        this.color = color;
        this.bodyType = bodyType;
        this.numberWheels = numberWheels;
        this.fuelType = fuelType;
        this.gearbox = gearbox;
        this.valueEngine = valueEngine;
    }

    public String getBrand() {
        return brand;
    }

    public String getModel() {
        return model;
    }

    public Color getColor() {
        return color;
    }

    public TypeBodyCar getBodyType() {
        return bodyType;
    }

    public int getNumberWheels() {
        return numberWheels;
    }

    public TypeFuel getFuelType() {
        return fuelType;
    }

    public TypeGearbox getGearbox() {
        return gearbox;
    }

    public double getValueEngine() {
        return valueEngine;
    }

    public void move() {
    }

    ;

    public void service() {
    }

    ;

    public void shiftGearbox() {
    }

    ;

    public void turnHeadlight() {
    }

    ;

    public void turnWipes() {
    }

    ;

    public void turnFoglights() {
    }

    ;

}