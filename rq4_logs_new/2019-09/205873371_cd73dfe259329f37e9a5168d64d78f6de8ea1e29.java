package com.kodilla.game;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.stage.Stage;

public class ElementalClash extends Application {



    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) throws Exception {
        Image backgroundImg = new Image("file:D:\\Coding\\mariusz-hajdyla-kodilla-game\\src\\main\\resources\\pics\\background.jpg");
        BackgroundSize backgroundSize = new BackgroundSize(100 , 100, true, true, true, false);
        BackgroundImage backgroundImage = new BackgroundImage(backgroundImg, BackgroundRepeat.NO_REPEAT, BackgroundRepeat.NO_REPEAT, BackgroundPosition.CENTER, backgroundSize);
        Background background = new Background(backgroundImage);

        GridPane grid = new GridPane();
        grid.setBackground(background);

        Scene scene = new Scene(grid, 1920, 1080, Color.BLACK);

        primaryStage.setTitle("ElementalClash");
        primaryStage.setScene(scene);
        primaryStage.show();

    }
}