package ch.heigvd.res.lab00;

public class Flute implements IInstrument{
    private String sound = "tweet";
    private String color = "orange";
    private int soundVolume = 1;

    public String play(){
        return sound;
    }

    public int getSoundVolume(){
        return soundVolume;
    }

    public String getColor(){
        return color;
    }
}