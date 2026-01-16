package com.tw.parking.boy;

import java.util.Comparator;
import java.util.List;

public class SmartParkingBoy {
    private final List<ParkingLot> parkingLots;


    public SmartParkingBoy(List<ParkingLot> parkingLots) {
        this.parkingLots = parkingLots;
    }

    public Ticket parking(Car car) {
        return parkingLots.stream().max(Comparator.comparingInt(ParkingLot::getCapacity))
                .map(parkingLot -> parkingLot.parking(car))
                .orElseThrow(NoCapacityException::new);
    }

    public Car takeCar(Ticket ticket) {
        return parkingLots.stream().filter(parkingLot -> parkingLot.hasCar(ticket)).findFirst()
                .map(parkingLot -> parkingLot.takeCar(ticket))
                .orElseThrow(InvalidTicketException::new);
    }
}