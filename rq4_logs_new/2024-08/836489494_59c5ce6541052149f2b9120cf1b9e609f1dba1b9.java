import java.util.Queue;

public class IntersectionManager {
    private VehicleQueueManager vehicleQueueManager;
    private TrafficLightManager trafficLightManager;
    private static final String[] DIRECTIONS = {"north", "south", "east", "west"};
    private boolean simulationRunning;
    private String currentGreenDirection;
    private long lastUpdateTime;
    private int currentLightDuration;
    private double speedMultiplier = 1.0;

    public IntersectionManager() {
        vehicleQueueManager = new VehicleQueueManager();
        trafficLightManager = new TrafficLightManager();
        currentGreenDirection = "north"; // Initial green direction
        simulationRunning = false;
        lastUpdateTime = System.currentTimeMillis();
        currentLightDuration = trafficLightManager.getGreenDuration();
    }

    public VehicleQueueManager getVehicleQueueManager() {
        return vehicleQueueManager;
    }

    public TrafficLightManager getTrafficLightManager() {
        return trafficLightManager;
    }

    public void setSimulationSpeed(double speedMultiplier) {
        this.speedMultiplier = speedMultiplier;
    }

    public void startSimulation(int numIterations) {
        System.out.println("Simulation started.");
        
        for (int i = 0; i < numIterations; i++) {
            // Get the current light status for the direction being simulated
            String lightColor = trafficLightManager.getLight("north"); // Adjust for other directions as needed
            
            if ("green".equals(lightColor)) {
                // While the light is green, keep dequeuing vehicles
                while (!vehicleQueueManager.isQueueEmpty("north")) {
                    Vehicle vehicle = vehicleQueueManager.removeVehicle("north");
                    System.out.println(vehicle.getType() + " removed from north queue.");
                    System.out.println(vehicle.getType() + " passed through north.");
                }
                // Change light to yellow after vehicles have passed
                trafficLightManager.setLight("north", "yellow");
                System.out.println("Set north light to yellow");
            } else {
                System.out.println("No vehicles can pass. Light is " + lightColor);
            }
            
            // Add a delay or sleep if needed here to simulate time passing
            try {
                Thread.sleep(1000); // Sleep for 1 second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        System.out.println("Simulation stopped.");
    }
    

    public void stopSimulation() {
        simulationRunning = false;
        System.out.println("Simulation stopped.");
    }

    private void updateTraffic() {
        long currentTime = System.currentTimeMillis();
        if ((currentTime - lastUpdateTime) / 1000 >= currentLightDuration / speedMultiplier) {
            lastUpdateTime = currentTime;
    
            String currentColor = trafficLightManager.getColor(currentGreenDirection);
    
            if (currentColor.equals("green")) {
                // Remove and process the first vehicle in the queue
                Vehicle removedVehicle = vehicleQueueManager.removeVehicle(currentGreenDirection);
                if (removedVehicle != null) {
                    System.out.println(removedVehicle + " passed through " + currentGreenDirection);
                } else {
                    System.out.println("No vehicles in the queue for " + currentGreenDirection);
                }
    
                // Change to yellow after processing vehicles
                trafficLightManager.setLight(currentGreenDirection, "yellow");
                currentLightDuration = trafficLightManager.getYellowDuration();
    
            } else if (currentColor.equals("yellow")) {
                // Change to red after yellow
                trafficLightManager.setLight(currentGreenDirection, "red");
                currentLightDuration = trafficLightManager.getRedDuration();
    
                // Determine the next green direction, prioritizing emergency vehicles
                boolean emergencyFound = false;
                for (String direction : DIRECTIONS) {
                    Queue<Vehicle> queue = vehicleQueueManager.viewQueue(direction);
                    if (queue != null && !queue.isEmpty() && queue.peek().isEmergency()) {
                        currentGreenDirection = direction;
                        emergencyFound = true;
                        break;
                    }
                }
    
                // If no emergency vehicle found, switch to the next direction in the order
                if (!emergencyFound) {
                    int currentIndex = (java.util.Arrays.asList(DIRECTIONS).indexOf(currentGreenDirection) + 1) % DIRECTIONS.length;
                    currentGreenDirection = DIRECTIONS[currentIndex];
                }
    
                // Set the new green light
                trafficLightManager.setLight(currentGreenDirection, "green");
                currentLightDuration = trafficLightManager.getGreenDuration();
                System.out.println("Green light now for " + currentGreenDirection);
            }
        }
    }
    
    

    private void checkEmergencyVehicles() {
        for (String direction : DIRECTIONS) {
            Queue<Vehicle> queue = vehicleQueueManager.viewQueue(direction);
            if (queue != null) {
                for (Vehicle vehicle : queue) {
                    if (vehicle.isEmergency()) {
                        System.out.println("Emergency vehicle in " + direction);
                        return; // Exit after finding an emergency vehicle
                    }
                }
            }
        }
    }
}
