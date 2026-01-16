package app;

import java.util.ArrayList;
import java.util.List;


public class Race {
    private static List<List<Vehicle>> vehicleTypes = new ArrayList<>();
    private static String[] types = {"Car", "Motorcycle", "Truck"};

    private static List<Vehicle> cars = new ArrayList<>();
    private static List<Vehicle> motors = new ArrayList<>();
    private static List<Vehicle> trucks = new ArrayList<>();

    protected static boolean isThereABrokenTruck = false;

    static {
        vehicleTypes.add(cars);
        vehicleTypes.add(motors);
        vehicleTypes.add(trucks);
    }

    private void createVehicles() {
        for (int i = 0; i < 10; i++) {
            cars.add(new Car());
            motors.add(new Motorcycle());
            trucks.add(new Truck());
        }
    }

    private void simulateRace() {
        for (int i = 0; i < 50; i++) {
            Weather.setRaining();
            searchForBrokenTruck();
            for (List<Vehicle> vehicleType : vehicleTypes) {
                for (Vehicle vehicle : vehicleType) {
                    vehicle.moveForAnHour(this);
                }
            }
        }
    }

    private void searchForBrokenTruck() {
        for (Vehicle truck : trucks) {
            if (truck.isBroken()) {
                isThereABrokenTruck = true;
                return;
            }
        }
        isThereABrokenTruck = false;
    }

    protected boolean isThereABrokenTruck() {
        return isThereABrokenTruck;
    }

    private void printRaceResults() {
        for (int i = 0; i < vehicleTypes.size(); i++) {
            System.out.println("\n" + types[i] + "s:");
            for (Vehicle vehicle : vehicleTypes.get(i)) {
                String result = vehicle.getName() + ": " + vehicle.getDistanceTraveled() + "km";
                System.out.println(result);
            }
        }
    }
    
    
// ---------------------------------------------------------
    public static void main(String[] args) {
        Race race = new Race();
        race.createVehicles();
        race.simulateRace();
        race.printRaceResults();
    }

}