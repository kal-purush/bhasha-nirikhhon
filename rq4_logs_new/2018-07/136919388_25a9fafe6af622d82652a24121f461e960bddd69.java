package com.kodilla.good.patterns.flights;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public final class AvailableFlights {

    private final List<Flight> listOfFlights = new ArrayList<>();

    public AvailableFlights () {
        listOfFlights.add(new Flight("Wrocław", "Gdańsk", LocalDate.now().plusDays(1L)));
        listOfFlights.add(new Flight("Wrocław", "Kraków", LocalDate.now().plusDays(4L)));
        listOfFlights.add(new Flight("Wrocław", "Warszawa", LocalDate.now().plusDays(7L)));
        listOfFlights.add(new Flight("Wrocław", "Lublin", LocalDate.now().plusDays(2L)));
        listOfFlights.add(new Flight("Wrocław", "Rzeszów", LocalDate.now().plusDays(3L)));
        listOfFlights.add(new Flight("Gdańsk", "Wrocław", LocalDate.now().plusDays(5L)));
        listOfFlights.add(new Flight("Gdańsk", "Kraków", LocalDate.now().plusDays(3L)));
        listOfFlights.add(new Flight("Gdańsk", "Katowice", LocalDate.now().plusDays(7L)));
        listOfFlights.add(new Flight("Gdańsk", "Lublin", LocalDate.now().plusDays(1L)));
        listOfFlights.add(new Flight("Gdańsk", "Rzeszów", LocalDate.now().plusDays(4L)));
        listOfFlights.add(new Flight("Lublin", "Wrocław", LocalDate.now().plusDays(4L)));
        listOfFlights.add(new Flight("Lublin", "Katowice", LocalDate.now().plusDays(5L)));
        listOfFlights.add(new Flight("Lublin", "Gdańsk", LocalDate.now().plusDays(2L)));
        listOfFlights.add(new Flight("Gdańsk", "Warszawa", LocalDate.now().plusDays(5L)));
        listOfFlights.add(new Flight("Lublin", "Warszawa", LocalDate.now().plusDays(3L)));
        listOfFlights.add(new Flight("Warszawa", "Rzeszów", LocalDate.now().plusDays(2L)));
        listOfFlights.add(new Flight("Gdańsk", "Poznań", LocalDate.now().plusDays(2L)));
        listOfFlights.add(new Flight("Poznań", "Kraków", LocalDate.now().plusDays(2L)));
        listOfFlights.add(new Flight("Kraków", "Warszawa", LocalDate.now().plusDays(1L)));
    }

    public List<Flight> getListOfFlights() {
        return new ArrayList<>(listOfFlights);
    }
}