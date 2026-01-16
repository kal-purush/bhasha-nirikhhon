package com.tw.parking.boy;

import java.util.List;

public class ParkingManager {
    private final List<ParkingBoy> parkingBoys;
    private final List<ParkingLot> parkingLots;

    public ParkingManager(List<ParkingBoy> parkingBoys, List<ParkingLot> parkingLots) {
        this.parkingBoys = parkingBoys;
        this.parkingLots = parkingLots;
    }

    public Ticket parking(Car car) {
        return parkingLots.stream().filter(ParkingLot::hasCapacity).findFirst()
                .map(parkingLot -> parkingLot.parking(car))
                .orElseGet(() -> parkingBoys.stream().filter(ParkingBoy::hasCapacity).findFirst()
                        .map(parkingBoy -> parkingBoy.parking(car))
                        .orElseThrow(NoCapacityException::new));
    }


    public Car takeCar(Ticket ticket) {
        return parkingLots.stream().filter(parkingLot -> parkingLot.hasCar(ticket)).findFirst()
                .map(parkingLot -> parkingLot.takeCar(ticket))
                .orElseGet(() -> parkingBoys.stream().filter(parkingBoy -> parkingBoy.hasCar(ticket)).findFirst()
                        .map(parkingBoy -> parkingBoy.takeCar(ticket))
                        .orElseThrow(InvalidTicketException::new));
    }
}