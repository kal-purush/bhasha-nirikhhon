package com.tw.parking.boy;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ParkingBoyTest {
    @Test
    void should_parking_1st_parking_lot_when_parking_given_manage_two_parking_lot_both_has_capacity() {
        Car car = new Car();
        ParkingLot firstParkingLot = new ParkingLot(10);
        ParkingLot secondParkingLot = new ParkingLot(10);
        List<ParkingLot> parkingLots = Lists.newArrayList(firstParkingLot, secondParkingLot);
        ParkingBoy parkingBoy = new ParkingBoy(parkingLots);

        Ticket ticket = parkingBoy.parking(car);

        assertNotNull(ticket);
        assertEquals(car, firstParkingLot.takeCar(ticket));
    }
}