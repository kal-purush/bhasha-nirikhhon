package com.apple.thinking.polymorphism.music;

/**
 * @author cl, lu.chen.@htouhui.com
 * @version 1.0
 */
public class Music {
    public static void tune(Instrument i) {
        i.play(Note.MIDDLE_C);
    }

    public static void main(String[] args) {
        Wind wind = new Wind();
        tune(wind);

        Stringed stringed = new Stringed();
        tune(stringed);

        Brass brass = new Brass();
        tune(brass);
    }
}