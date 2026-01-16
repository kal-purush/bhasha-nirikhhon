package com.bradleyboxer.scavengerhunt;

import java.io.Serializable;

public class ClueSegment implements Serializable{
    private GeofenceData geofenceData;
    private String clueText;
    private boolean solved = false;

    public ClueSegment(String clueText, GeofenceData geofenceData) {
        this.clueText = clueText;
        this.geofenceData = geofenceData;
    }

    public String getClueText() {
        return clueText;
    }

    public GeofenceData getGeofenceData() {
        return geofenceData;
    }

    public void setSolved() {
        solved = true;
    }

    public boolean isSolved() {
        return solved;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof ClueSegment)) return false;
        ClueSegment other = (ClueSegment) obj;
        return other.getGeofenceData().equals(this.getGeofenceData()) && other.getClueText().equals(this.getClueText());
    }
}