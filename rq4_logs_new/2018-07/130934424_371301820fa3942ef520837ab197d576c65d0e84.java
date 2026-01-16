package com.bradleyboxer.scavengerhunt;

import java.io.Serializable;

public class Clue implements Serializable {
    private String geofenceText; //"clue discovered"
    private String compassText; //"clue solved/found"
    private GeofenceData geofenceClue;
    private GeofenceData compassClue;
    private boolean discovered;
    private boolean solved;

    public Clue(String geofenceText, GeofenceData geofenceClue, String compassText, GeofenceData compassClue) {
        this.geofenceText = geofenceText;
        this.geofenceClue = geofenceClue;
        this.compassText = compassText;
        this.compassClue = compassClue;
    }

    public Clue(String geofenceText, GeofenceData geofenceClue) {
        this.geofenceText = geofenceText;
        this.geofenceClue = geofenceClue;
        this.compassText = geofenceText;
        this.compassClue = geofenceClue;
    }

    public GeofenceData getGeofenceClue() {
        return geofenceClue;
    }

    public GeofenceData getCompassClue() {
        return compassClue;
    }

    public String getClueDiscoveredText() {
        return geofenceText;
    }

    public String getClueSolvedText() {
        return compassText;
    }

    public void setDiscovered() {
        discovered = true;
    }

    public void setSolved() {
        solved = true;
    }

    public boolean hasBeenDiscovered() {
        return discovered;
    }

    public boolean hasBeenSolved() {
        return solved;
    }
}