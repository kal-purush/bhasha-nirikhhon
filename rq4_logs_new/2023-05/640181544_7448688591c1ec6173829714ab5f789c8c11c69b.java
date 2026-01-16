package Pages.Burak;

import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.Clip;
import java.io.File;

public class MusicPlayer extends Thread {
    private String musicFilePath;

    public MusicPlayer(String musicFilePath) {
        this.musicFilePath = musicFilePath;
    }

    @Override
    public void run() {
        try {
            File soundFile = new File(musicFilePath);
            AudioInputStream audioInputStream = AudioSystem.getAudioInputStream(soundFile);
            Clip clip = AudioSystem.getClip();
            clip.open(audioInputStream);
            clip.start();
            // Wait until the music finishes playing
            Thread.sleep(clip.getMicrosecondLength() / 1000);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}