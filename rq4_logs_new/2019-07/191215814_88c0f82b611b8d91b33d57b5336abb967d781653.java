package com.epam.brest.summer.courses2019.model;

import java.time.LocalDate;

public class Trip {

    private Integer tripId;
    private LocalDate dataTrip;
    private Integer carId;
    private Integer distance;
    private String tripStatus;

    public Integer getTripId() {
        return tripId;
    }

    public void setTripId(Integer tripId) {
        this.tripId = tripId;
    }

    public LocalDate getDataTrip() {
        return dataTrip;
    }

    public void setDataTrip(LocalDate dataTrip) {
        this.dataTrip = dataTrip;
    }

    public Integer getCarId() {
        return carId;
    }

    public void setCarId(Integer carId) {
        this.carId = carId;
    }

    public Integer getDistance() {
        return distance;
    }

    public void setDistance(Integer distance) {
        this.distance = distance;
    }

    public String getTripStatus() {
        return tripStatus;
    }

    public void setTripStatus(String tripStatus) {
        this.tripStatus = tripStatus;
    }
}