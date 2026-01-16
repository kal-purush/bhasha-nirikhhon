package com.verrev.code.radiostation;

/**
 * Created by Vootele Verrev on 15-Oct-15.
 */
public class ImprovedRadioStation extends RadioStation {
    private String currentSongName;
    private String currentSongAuthor;

    public ImprovedRadioStation(String name, float frequency) {
        super(name, frequency);
        this.currentSongAuthor = "Normal";
        this.currentSongName = "Everyday guy";
    }

    public String getCurrentSongInfo() {
        return currentSongName + " - " + currentSongAuthor;
    }

    @Override
    public String toString() {
        String s = super.toString();
        if (!isInEmergencyMode) return s + " Current song info: " + getCurrentSongInfo();
        return s;
    }
}