package de.hebk.sound;

import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.util.Duration;

import java.io.File;

public class SoundManager {
    private MediaPlayer player;

    public void playSound(SoundType type, boolean repeating) {
        String path = getClass().getResource("").getFile();

        switch (type) {
            case QUESTION -> path += "question.wav";
            case RIGHT_ANSWER -> path += "right_answer.wav";
            case WRONG_ANSWER -> path += "wrong_answer.wav";
            case HALF_JOKER -> path += "half_joker.wav";
            case TELEPHONE_JOKER -> path += "telephone_joker.wav";
            case WIN -> path += "win.wav";
            case AUDIENCE_JOKER -> path += "audience_joker.wav";
            case START -> path += "start.wav";
        }

        if (repeating) {
            Media media = new Media(new File(path).toURI().toString());
            player = new MediaPlayer(media);

            player.setOnEndOfMedia(new Runnable() {
                public void run() {
                    player.seek(Duration.ZERO);
                }
            });
            player.play();
        }
        else {
            Media media = new Media(new File(path).toURI().toString());
            player = new MediaPlayer(media);
            player.play();
        }
    }

    public boolean isStopped() {
        return player == null;
    }

    public void stopSound() {
        if (player != null) {
            player.stop();
            player = null;
        }
    }

    public void playNext(SoundType type, boolean repeating) {
        player.setOnEndOfMedia(() -> {
            playSound(type, repeating);
        });
    }
}