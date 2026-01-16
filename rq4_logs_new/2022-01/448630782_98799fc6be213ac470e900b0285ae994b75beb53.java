package net.htlgkr.groupK.chess;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;

import java.io.IOException;
import java.net.URL;

public class HelloApplication extends Application {
    @Override
    public void start(Stage stage) throws IOException
    {
        //createMenuScreen(stage);
        createChessGame(stage);
    }

    public static void main(String[] args) {
        launch();
    }

    public static void createMenuScreen(Stage stageMenuScreen) throws IOException
    {
        FXMLLoader menuScreen = new FXMLLoader(HelloApplication.class.getResource("menu-screen.fxml"));
        Scene sceneMenuScreen = new Scene(menuScreen.load(), 600, 400);
        stageMenuScreen.setScene(sceneMenuScreen);
        stageMenuScreen.show();
    }

    public static void createChessGame(Stage stageChessGame) throws IOException
    {
        FXMLLoader chessGame = new FXMLLoader(HelloApplication.class.getResource("chess-game.fxml"));
        Scene sceneChessGame = new Scene(chessGame.load(), 500, 700);
        stageChessGame.setScene(sceneChessGame);

        stageChessGame.setResizable(false);
        Image icon = new Image(HelloApplication.class.getResource("/images/chess_icon.jpg").toString());
        stageChessGame.getIcons().add(icon);
        stageChessGame.setTitle("Chess Game");

        stageChessGame.show();
    }
}