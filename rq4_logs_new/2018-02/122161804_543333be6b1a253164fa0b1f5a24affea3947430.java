package ch.heigvd.res.lab00;

public class AeolianHarp implements IInstrument {

    private String sound;
    private int volume;
    private String color;

    public AeolianHarp(String sound, int volume, String color)
    {
        this.sound = sound;
        this.volume = volume;
        this.color = color;
    }

    public AeolianHarp() {
        this("ding", 3, "silver");
    }


    @Override
    public String play()
    {
        return sound;
    }

    @Override
    public int getSoundVolume()
    {
        return volume;
    }

    @Override
    public String getColor()
    {
        return color;
    }
}