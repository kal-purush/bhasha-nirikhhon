package com.dice.dicemagic;

import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.Clip;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Audio {
    Map<String, Clip> clips = new HashMap<>();
    public Audio(){
        loadAudioClip("audio/bell.wav");
        loadAudioClip("audio/pops.wav");
        loadAudioClip("audio/rolling-dice.wav");
    }

    public void loadAudioClip(String soundName){
        try
        {
            AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(new File(soundName).getAbsoluteFile());
            Clip clip = AudioSystem.getClip();
            clip.open(audioInputStream);
            clips.put(soundName, clip);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public void playAudioClip(String soundName){
        Clip c = clips.get(soundName);
        c.setFramePosition(0);
        c.start();
    }
}