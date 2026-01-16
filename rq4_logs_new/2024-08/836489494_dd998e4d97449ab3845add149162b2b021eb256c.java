public class Intersection {
    private TrafficLight northLight;
    private TrafficLight southLight;
    private TrafficLight eastLight;
    private TrafficLight westLight;
    private VehicleQueue northQueue;
    private VehicleQueue southQueue;
    private VehicleQueue eastQueue;
    private VehicleQueue westQueue;

    public Intersection() {
        northLight = new TrafficLight(30, 5, 25);
        southLight = new TrafficLight(30, 5, 25);
        eastLight = new TrafficLight(30, 5, 25);
        westLight = new TrafficLight(30, 5, 25);
        northQueue = new VehicleQueue("north");
        southQueue = new VehicleQueue("south");
        eastQueue = new VehicleQueue("east");
        westQueue = new VehicleQueue("west");
    }

    public void setLight(String direction, String color) {
        switch (direction.toLowerCase()) {
            case "north":
                northLight.setColor(color);
                break;
            case "south":
                southLight.setColor(color);
                break;
            case "east":
                eastLight.setColor(color);
                break;
            case "west":
                westLight.setColor(color);
                break;
            default:
                System.out.println("Invalid direction.");
                break;
        }
    }

    public void addVehicle(String direction, Vehicle vehicle) {
        switch (direction.toLowerCase()) {
            case "north":
                northQueue.addVehicle(vehicle);
                break;
            case "south":
                southQueue.addVehicle(vehicle);
                break;
            case "east":
                eastQueue.addVehicle(vehicle);
                break;
            case "west":
                westQueue.addVehicle(vehicle);
                break;
            default:
                System.out.println("Invalid direction.");
                break;
        }
    }

    public void removeVehicle(String direction) {
        switch (direction.toLowerCase()) {
            case "north":
                northQueue.removeVehicle();
                break;
            case "south":
                southQueue.removeVehicle();
                break;
            case "east":
                eastQueue.removeVehicle();
                break;
            case "west":
                westQueue.removeVehicle();
                break;
            default:
                System.out.println("Invalid direction.");
                break;
        }
    }

    public void viewQueue(String direction) {
        switch (direction.toLowerCase()) {
            case "north":
                northQueue.viewQueue();
                break;
            case "south":
                southQueue.viewQueue();
                break;
            case "east":
                eastQueue.viewQueue();
                break;
            case "west":
                westQueue.viewQueue();
                break;
            default:
                System.out.println("Invalid direction.");
                break;
        }
    }

    public boolean areLightsSet() {
        return northLight.isColorSet() && southLight.isColorSet() &&
               eastLight.isColorSet() && westLight.isColorSet();
    }

    public boolean areTimingsSet() {
        return northLight.isTimingSet() && southLight.isTimingSet() &&
               eastLight.isTimingSet() && westLight.isTimingSet();
    }

    public boolean areVehiclesPresent() {
        return northQueue.getQueueSize() > 0 || southQueue.getQueueSize() > 0 ||
               eastQueue.getQueueSize() > 0 || westQueue.getQueueSize() > 0;
    }
}