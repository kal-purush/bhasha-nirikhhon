package com.kodilla.exception.test;

import java.util.HashMap;
import java.util.Map;

public class FlightFinder {
    private Map<String, Boolean> flights = initFlights();

    public boolean findFlight(Flight flight) throws RouteNotFoundException {
        if (flights.containsKey(flight.getArrivalAirport())) {
            return flights.get(flight.getArrivalAirport());
        } else {
            throw new RouteNotFoundException("Arrival airport is not on the list");
        }
    }

    private Map<String, Boolean> initFlights() {
        Map<String, Boolean> flights = new HashMap<>();

        flights.put("Heathrow", true);
        flights.put("Lotnisko Chopina", false);
        flights.put("Moskwa-Szeremietiewo", true);
        flights.put("Johannesburg", false);
        flights.put("Nowy Jork-JFK", true);

        return flights;
    }
}