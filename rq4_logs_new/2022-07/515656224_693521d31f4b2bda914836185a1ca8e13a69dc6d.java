package ru.netolody.radio;

public class Radio {

    private int channel;

    public void setChannel(int channel) {
        if (channel < 0) {
            return;
        }
        if (channel > 9) {
            return;
        }
        this.channel = channel;
    }

    public int getChannel () {
        return channel;
    }
}