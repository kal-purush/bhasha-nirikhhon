package tictactoe.ui;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Background;
import javafx.scene.layout.BackgroundFill;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.CornerRadii;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.paint.LinearGradient;
import javafx.scene.paint.Stop;
import javafx.stage.Stage;
import javafx.util.Duration;

public class Splash extends BorderPane {

    protected final VBox vBox;
    protected final ImageView imgView;
    protected final ProgressBar progressBar;

    public Splash(Stage primaryStage) {

        vBox = new VBox();
        imgView = new ImageView();
        progressBar = new ProgressBar();

        setMaxHeight(USE_PREF_SIZE);
        setMaxWidth(USE_PREF_SIZE);
        setMinHeight(USE_PREF_SIZE);
        setMinWidth(USE_PREF_SIZE);
        setPrefHeight(400.0);
        setPrefWidth(600.0);

        Stop[] stops = new Stop[] {
            new Stop(0, Color.web("#cce4f6")),
            new Stop(1, Color.web("#377ebc"))
        };
        LinearGradient gradient = new LinearGradient(
                0, 0, 0, 1,
                true,
                javafx.scene.paint.CycleMethod.NO_CYCLE,
                stops
        );
        Background background = new Background(new BackgroundFill(gradient, CornerRadii.EMPTY, Insets.EMPTY));
        setBackground(background);

        BorderPane.setAlignment(vBox, Pos.CENTER);

        vBox.setAlignment(Pos.CENTER);
        vBox.setSpacing(20);  

        imgView.setFitHeight(getPrefHeight()/2);  
        imgView.setFitWidth(getPrefWidth()/2);    
        imgView.setPickOnBounds(true);
        imgView.setPreserveRatio(true);

        
        try {
            imgView.setImage(new Image(getClass().getResource("/resources/images/logo.png").toExternalForm()));
        } catch (NullPointerException e) {
            System.err.println("Image is not found");
        }

        progressBar.setPrefWidth(getPrefWidth()/2);  
        progressBar.setProgress(0.0);
        progressBar.setStyle("-fx-accent: #4caf50; -fx-control-inner-background: #ffffff;");

        vBox.getChildren().add(imgView);
        vBox.getChildren().add(progressBar);

        setCenter(vBox);

        simulateProgress(primaryStage);
    }

    private void simulateProgress(Stage primaryStage) {
        Timeline timeline = new Timeline();
        timeline.setCycleCount(100);

        timeline.getKeyFrames().add(
                new KeyFrame(
                        Duration.millis(20),
                        event -> {
                            double currentProgress = progressBar.getProgress();
                            progressBar.setProgress(currentProgress + 0.01);
                        }
                )
        );

        timeline.setOnFinished(event -> loadNextPage(primaryStage));
        timeline.play();
    }

    private void loadNextPage(Stage stage) {
        stage.setScene(new Scene(new Home(stage), 800, 600));
    }
}