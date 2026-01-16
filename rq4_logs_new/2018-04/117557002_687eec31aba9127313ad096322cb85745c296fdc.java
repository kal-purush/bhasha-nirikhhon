package com.software.beacon;

public class BeaconDurationCalculator {
    private static final int DURATION = 15000;
    private long activatedAt = Long.MAX_VALUE;
    private String beaconId = "0";
    private double beaconDistance = 0.0;


    public void activate(String beaconId, double beaconDistance) {
        this.beaconId = beaconId;
        this.beaconDistance = beaconDistance;
    }

    public String getBeaconId() {
        return beaconId;
    }

    public double getBeaconDistance() {
        return beaconDistance;
    }
}