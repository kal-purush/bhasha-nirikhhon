package com.example.multithreadingProject.NullObject;

public class NullObjectApplication {
    public static void main(String[] args) {
        VehicleFactory vehicleFactory = new VehicleFactory();

        Vehicle car = vehicleFactory.getVehicle("Car");
        printAttributes(car);

        Vehicle bike = vehicleFactory.getVehicle("Bike");
        printAttributes(bike);
    }

    public static void printAttributes(Vehicle vehicle) {
        System.out.printf("Capacity: %d, Tank: %d%n", vehicle.getSeatingCapacity(), vehicle.getTankCapacity());
    }
}