package com.bogdan.battleship;

import com.bogdan.battleship.controller.SceneController;
import javafx.application.Application;
import javafx.stage.Stage;

import java.io.IOException;

public class Client extends Application {

    public static final int WIDTH = 720;
    public static final int HEIGHT = 480;

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage stage) throws IOException {
        SceneController sceneController = new SceneController();
        sceneController.switchToMenu(stage, WIDTH, HEIGHT);
    }
}