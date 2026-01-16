package com.kodilla.exception.test;

import java.util.HashMap;
import java.util.Map;

public class AirportsDatabase {
    private Map<String, Destinations> airportsDatabase = new HashMap<>();

    public void addDestinationsMap (String departureAirport, Destinations destinations) {
        airportsDatabase.put(departureAirport, destinations);
    }

    public Destinations getDestinationsMap (String departureAirport) throws DepartureNotFoundException {
        Destinations result = new Destinations();

        if (!airportsDatabase.keySet().contains(departureAirport)) {
            throw new DepartureNotFoundException();
        }

        for (Map.Entry<String, Destinations> entry: airportsDatabase.entrySet()) {
            if (entry.getKey().equals(departureAirport)) {
                result = entry.getValue();
            }
        }
        return result;
    }
}