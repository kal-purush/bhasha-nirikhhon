package com.example.groupnameapi.controller;

import com.example.groupnameapi.classes.Passenger;
import com.example.groupnameapi.handlers.PassengerHandler;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;

@RestController
@RequestMapping("/passengers")
public class PassengerRestController {
    PassengerHandler passengerHandler = new PassengerHandler();

    @RequestMapping(path="", method = RequestMethod.POST)
    public String createPassenger(@RequestBody Passenger passenger) {
        System.out.println("Connected.");
        passengerHandler.addPassenger(passenger);
        return "Passenger " + passenger.getId() + " added.";
    }

    @RequestMapping(path="/{id}", method = RequestMethod.GET)
    public Passenger findOnePassenger(@PathVariable int id) {
        return passengerHandler.findPassengerById(id);
        // UI will take in passenger object as JSON object, parse into object and print out values
    }

    @RequestMapping(path="" , method = RequestMethod.GET)
    public ArrayList<Passenger> findAllPassengers() {
        return passengerHandler.findAllPassengers();
    }

    @RequestMapping(path="/{id}", method = RequestMethod.PUT)
    public String updatePassenger(@PathVariable int id, @RequestBody Passenger passenger) {
        passenger.setId(id);
        passengerHandler.updatePassenger(passenger);
        return "Passenger " + id + " updated.";
    }

    @RequestMapping(path="/{id}/{password}", method = RequestMethod.DELETE)
    public String deletePassenger(@PathVariable int id, @PathVariable String password) {
        passengerHandler.removePassenger(id, password);
        return "Passenger " + id + "removed.";
    }


}