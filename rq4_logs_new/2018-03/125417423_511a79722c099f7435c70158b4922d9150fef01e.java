package com.seanmclane.guitar_tuner;

/**
 * Created by per6 on 3/15/18.
 */

public class Note {
    private String noteName;
    private double noteFrequency;

    public Note(String noteName, double noteFrequency) {
        this.noteName = noteName;
        this.noteFrequency = noteFrequency;
    }

    public String getNoteName() {
        return noteName;
    }

    public void setNoteName(String noteName) {
        this.noteName = noteName;
    }

    public double getNoteFrequency() {
        return noteFrequency;
    }

    public void setNoteFrequency(double noteFrequency) {
        this.noteFrequency = noteFrequency;
    }
}