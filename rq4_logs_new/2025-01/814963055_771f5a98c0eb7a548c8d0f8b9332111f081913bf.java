package com.example.multithreadingProject.ParkingLot;

import com.example.multithreadingProject.ParkingLot.models.*;
import com.example.multithreadingProject.ParkingLot.services.CostCalculationService;
import com.example.multithreadingProject.ParkingLot.services.EightWheelerCostCalculator;
import com.example.multithreadingProject.ParkingLot.services.FourWheelerCostCalculator;
import com.example.multithreadingProject.ParkingLot.services.TwoWheelerCostCalculator;

import java.util.*;

public class ParkingLotApplication {

    public static void main(String[] args) throws InterruptedException {
        Map<Integer, ParkingFloor> floorMap = new HashMap<>();
        floorMap.put(1, new ParkingFloor(3));
        floorMap.put(2, new ParkingFloor(1));
        floorMap.put(3, new ParkingFloor(2));

        Map<String, CostCalculationService> serviceMap = new HashMap<>();
        serviceMap.put("two-wheeler", new TwoWheelerCostCalculator());
        serviceMap.put("four-wheeler", new FourWheelerCostCalculator());
        serviceMap.put("eight-wheeler", new EightWheelerCostCalculator());

        ParkingLot parkingLot = new ParkingLot(serviceMap);
        parkingLot.setFloorMap(floorMap);

        List<Vehicle> vehicles = new ArrayList<>();
        vehicles.add(new TwoWheelerVehicle("1"));
        vehicles.add(new EightWheelerVehicle("1"));
        vehicles.add(new TwoWheelerVehicle("1"));
        vehicles.add(new FourWheelerVehicle("1"));
        vehicles.add(new FourWheelerVehicle("1"));
        vehicles.add(new EightWheelerVehicle("1"));

        List<Ticket> tickets = new ArrayList<>();

        for(Vehicle vehicle: vehicles) {
            tickets.add(parkingLot.parkVehicle(vehicle));
            Thread.sleep(300);
        }

        for(Ticket ticket: tickets) {
            System.out.println(ticket.toString());
        }

        parkingLot.unparkVehicleAndGetCost(tickets.get(5));
        Ticket ticket = parkingLot.parkVehicle(vehicles.get(5));
        System.out.println(ticket);
        parkingLot.unparkVehicleAndGetCost(tickets.get(3));
        Ticket ticket1 = parkingLot.parkVehicle(vehicles.get(3));
        System.out.println(ticket1);
    }
}