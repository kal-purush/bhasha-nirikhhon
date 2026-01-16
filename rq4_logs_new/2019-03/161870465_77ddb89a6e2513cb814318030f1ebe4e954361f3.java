package com.kren.pattern.command.applience;

public interface Audio {
    void chooseRadio();
    void chooseCd();
    void setVolume(int volume);
    void setTrack(String name);
    void off();
}