package travelAgency;

import java.util.ArrayList;
import java.util.List;

public class Activity {
	String name;
    String description;
    double cost;
    int capacity;
    Destination destination;
    List<Passenger> passengers;

    Activity(String name, String description, double cost, int capacity, Destination destination) {
        this.name = name;
        this.description = description;
        this.cost = cost;
        this.capacity = capacity;
        this.destination = destination;
        passengers = new ArrayList<>();
    }

    void enrollPassenger(Passenger passenger) {
        if (passengers.size() < capacity) {
            passengers.add(passenger);
            passenger.activities.add(this);
        }
    }


}