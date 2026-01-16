package com.fiuba.tdpii.correapp.models.web;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class DriversByTrip {


    @SerializedName("driverId")
    @Expose
    private Long driverId;
    @SerializedName("distFrom")
    @Expose
    private Float distFrom;

    @SerializedName("ring")
    @Expose
    private Long ring;

    public Long getDriverId() {
        return driverId;
    }

    public void setDriverId(Long driverId) {
        this.driverId = driverId;
    }

    public Float getDistFrom() {
        return distFrom;
    }

    public void setDistFrom(Float distFrom) {
        this.distFrom = distFrom;
    }


    public Long getRing() {
        return ring;
    }

    public void setRing(Long ring) {
        this.ring = ring;
    }
}