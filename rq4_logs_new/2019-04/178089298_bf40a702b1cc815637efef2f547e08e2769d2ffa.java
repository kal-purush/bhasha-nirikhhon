package com.fiuba.tdpii.correapp.models.web;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Rejected {
    @SerializedName("driver_id")
    @Expose
    private Long driverId;
    @SerializedName("id")
    @Expose
    private Long id;
    @SerializedName("trip_id")
    @Expose
    private Long tripId;
    @SerializedName("comment")
    @Expose
    private String comment;

    public Long getDriverId() {
        return driverId;
    }

    public void setDriverId(Long driverId) {
        this.driverId = driverId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getTripId() {
        return tripId;
    }

    public void setTripId(Long tripId) {
        this.tripId = tripId;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}