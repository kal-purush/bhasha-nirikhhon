package com.example.sanbotapp.robotControl;

import android.media.AudioManager;

public class AudioControl {
    private AudioManager audioManager;

    public AudioControl(AudioManager audioManager){
        this.audioManager = audioManager;
    }

    // Función utilizada para guardar el volumen de los altavoces del robot
    public void setVolumen(int volumen) {
        audioManager.setStreamVolume(AudioManager.STREAM_MUSIC, volumen, 0);
    }

    // Función utilizada para obtener el volumen de los altavoces del robot
    public int getVolumen() {
        return audioManager.getStreamVolume(AudioManager.STREAM_MUSIC);
    }

}