package tictactoe.ui.screens;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.scene.Scene;
import javafx.stage.Stage;
import javafx.util.Duration;
import tictactoe.domain.model.Record;
import tictactoe.domain.model.Tile;
import tictactoe.domain.usecases.GetXOImageUseCase;

public class ReplayGame extends Board {

    private Record gameRecord;
    private boolean isX;
    private Tile tile;
    private Timeline timeline;

    public ReplayGame(Stage stage, Record gameRecord) {
        super(stage);
        this.gameRecord = gameRecord;
        selectWineerPlayer();
        recordBtn.setDisable(true);
        forfeitBtn.setDisable(true);
        replayMoves();
    }

    private void selectWineerPlayer() {
        userNamePlayer1.setText(gameRecord.getUser1());
        userNamePlayer2.setText(gameRecord.getUser2());

        switch (gameRecord.getWinner()) {
            case 'W': {
                player1Score.setText("Wineer");
                player2Score.setText(null);
                player1Score.setTextFill(javafx.scene.paint.Color.valueOf("#28a745")); //green
                break;
            }
            case 'L': {
                player2Score.setText("Wineer");
                player1Score.setText(null);
                player2Score.setTextFill(javafx.scene.paint.Color.valueOf("#28a745")); //red
                break;
            }
            case 'E': {
                player1Score.setText("Draw");
                player2Score.setText("Draw");
                player1Score.setTextFill(javafx.scene.paint.Color.valueOf("#ffa500")); //orange
                player2Score.setTextFill(javafx.scene.paint.Color.valueOf("#ffa500"));
                break;
            }
        }

    }

    private void replayMoves() {
        int[] positions = getPositionsArray();
        timeline = new Timeline();
        for (int i = 0; i < positions.length; i++) {
            int index = i;
            KeyFrame keyFrame = new KeyFrame(
                    Duration.seconds(index * 2),
                    print -> {
                        tile = tiles.get(positions[index]);
                        if (index % 2 != 0) {
                            tile.getBtn().setGraphic(GetXOImageUseCase.getXOImage(isX));
                            playSound.playSound(2);
                        } else {
                            tile.getBtn().setGraphic(GetXOImageUseCase.getXOImage(!isX));
                            playSound.playSound(1);
                        }
                    }
            );
            timeline.getKeyFrames().add(keyFrame);
        }
        timeline.play();
        timeline.setOnFinished(delay -> {
            new Timeline(new KeyFrame(
                    Duration.seconds(2),
                    move -> loadNextPage()
            )).play();
        });

    }

    private int[] getPositionsArray() {
        String positions = gameRecord.getPsitions();
        int[] positionArray = new int[positions.length()];
        for (int i = 0; i < positions.length(); i++) {
            positionArray[i] = Character.getNumericValue(positions.charAt(i)) - 1;
        }
        return positionArray;
    }

    private void loadNextPage() {
        Scene scene = new Scene(new OfflineBase(stage), 800, 600);
        stage.setScene(scene);
        scene.getStylesheets().add(getClass().getResource("/resources/style/style.css").toExternalForm());
    }

}