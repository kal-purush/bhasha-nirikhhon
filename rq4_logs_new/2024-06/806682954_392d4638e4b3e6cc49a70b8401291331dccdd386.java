package lv.acodemy.classroom;

import java.sql.SQLOutput;

public class Car {
    // Class fields

    private String carBrand;
    private String model;
    private int productionYear;
    private double mileage;
    private String color;
    private boolean isRunning = false;

    public Car(String carBrand, String model, int productionYear, double mileage, String color) {
        this.carBrand = carBrand;
        this.model = model;
        this.productionYear = productionYear;
        this.mileage = mileage;
        this.color = color;
    }

    private Car(CarBuilder builder) {
        this.carBrand = builder.carBrand;
        this.model = builder.model;
        this.productionYear = builder.productionYear;
        this.mileage = builder.mileage;
        this.color = builder.color;
    }

    public Car() {
    }

    @Override
    public String toString() {
        return "Car{" +
                "carBrand='" + carBrand + '\'' +
                ", model='" + model + '\'' +
                ", productionYear=" + productionYear +
                ", mileage=" + mileage +
                ", color='" + color + '\'' +
                '}';
    }

    public void start() {
        if(!isRunning) {
            isRunning = true;
            System.out.println("The car has started.");
        } else {
            System.out.println("The car is already running!");
        }
    }

    public void stop() {
        if(isRunning){
            isRunning = false;
            System.out.println("The car has been stopped!");
        } else {
            System.out.println("The car is already stopped!");
        }
    }

    public void drive(double kilometers) {
        if(isRunning) {
            this.mileage = this.mileage + kilometers;
            System.out.println("The mileage is: " + this.mileage);
            System.out.println(String.format("The car has driven %.2f kilometers. Total mileage is: %.2f %n", kilometers, this.mileage));
        }
    }

    // Getters

    public String getCarBrand() {
        return carBrand;
    }

    public String getModel() {
        return model;
    }

    public int getProductionYear() {
        return productionYear;
    }

    public double getMileage() {
        return mileage;
    }

    // Setters

    public void setCarBrand(String carBrand) {
        this.carBrand = carBrand;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public void setMileage(double mileage) {
        this.mileage = mileage;
    }

    public void setProductionYear(int productionYear) {
        this.productionYear = productionYear;
    }

    public String getColor() {
        return color;

    }
    public static class CarBuilder {

        private String carBrand;
        private String model;
        private int productionYear;
        private double mileage;
        private String color;

        public CarBuilder() {
        }

        public CarBuilder setCarBrand(String carBrand) {
            this.carBrand = carBrand;
            return this;
        }

        public CarBuilder setModel(String model) {
            this.model = model;
            return this;
        }

        public CarBuilder setProductionYear(int productionYear) {
            this.productionYear = productionYear;
            return this;
        }

        public CarBuilder setMileage(double mileage) {
            this.mileage = mileage;
            return this;
        }

        public CarBuilder setColor(String color) {
            this.color = color;
            return this;
        }

        public Car build() {
            return new Car (this);
        }
    }
}