package com.bmcc.util;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import javax.sound.sampled.*;

public class GameAudio {


    public static void PlayWelcomeAudio() throws UnsupportedAudioFileException, IOException, LineUnavailableException{
        Scanner scanner = new Scanner(System.in);

        File file = new File("asset/RagingStreets.wav");
        AudioInputStream audioStream = AudioSystem.getAudioInputStream(file);
        Clip clip = AudioSystem.getClip();
        clip.open(audioStream);
        clip.start();

    }

}
