package tictactoe.domain.usecases;

import java.io.File;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;

public class PlayBackgroundMusicUseCase {

    private static final File BACKGROUND_SOUND = new File("src/resources/sounds/background.wav");
    private MediaPlayer backgroundPlayer;
    private static PlayBackgroundMusicUseCase instance;

    private PlayBackgroundMusicUseCase() {
    }

    public static PlayBackgroundMusicUseCase getInstance() {
        if (instance == null) {
            instance = new PlayBackgroundMusicUseCase();
        }
        return instance;
    }

    public void startBackgroundMusic() {
        if (backgroundPlayer == null || backgroundPlayer.getStatus() == MediaPlayer.Status.DISPOSED) {
            Media media = new Media(BACKGROUND_SOUND.toURI().toString());
            backgroundPlayer = new MediaPlayer(media);

            backgroundPlayer.setOnEndOfMedia(() -> backgroundPlayer.play());
        }
        if (backgroundPlayer.getStatus() != MediaPlayer.Status.PLAYING) {
            backgroundPlayer.play();
        }
    }

    public void stopBackgroundMusic() {
        if (backgroundPlayer != null) {
            if (backgroundPlayer.getStatus() == MediaPlayer.Status.PLAYING) {
                backgroundPlayer.stop();
            }
        }
    }
}