package com.example.acciojob_brickbreakergame;

import javafx.application.Application;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;

public class HelloApplication extends Application {
    static Stage globalstage;
    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(HelloApplication.class.getResource("startPage.fxml"));
        Scene scene = new Scene(fxmlLoader.load(), 600, 400);
        globalstage = stage;
        stage.setTitle("Welcome to the smash");
        stage.setScene(scene);
        stage.show();
    }

    @FXML
    public void startbuttonclick() throws IOException {
        //we have to create game interface
//        globalstage.setTitle("button onHelloButtonClick");
        FXMLLoader fxmlLoader = new FXMLLoader(HelloApplication.class.getResource("gamescreen.fxml"));
        Scene scene = new Scene(fxmlLoader.load(), 600, 400);

        globalstage.setScene(scene);
        globalstage.setTitle("Welcome to the arena");
        globalstage.show();
    }

    public static void main(String[] args) {
        launch();
    }
}