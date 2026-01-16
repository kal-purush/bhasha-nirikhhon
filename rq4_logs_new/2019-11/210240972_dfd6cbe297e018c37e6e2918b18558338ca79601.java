package com.example.trippe.model;

import androidx.annotation.NonNull;

import java.util.Date;
import java.util.TimeZone;

public class NationalTrip extends Trip {

    public NationalTrip(String tripId, int tripFlagIndicator, String startDate, String endDate, Location destination, int milesAwayFromHome, TimeZone timeZone) {
        super(tripId, tripFlagIndicator, startDate, endDate, destination, milesAwayFromHome, timeZone);

    }

    @NonNull
    @Override
    public String toString() {
        return destination.getCity() + ", " + this.destination.getState();
    }

}