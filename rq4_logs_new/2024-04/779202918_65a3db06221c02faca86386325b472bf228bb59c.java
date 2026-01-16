package com.example.englishforkids;

import javafx.event.ActionEvent;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;

import java.io.IOException;
import java.io.InputStream;

public class ShowNewScene {
    public static void show(FXMLLoader loader, String title){
        try {
            Parent root = loader.load();
            Stage newStage = new Stage();
            newStage.setTitle(title);
            newStage.setScene(new Scene(root));

            String imagePath = "/img/main_ico.png";
            InputStream inputStream = ShowNewScene.class.getResourceAsStream(imagePath);
            Image icon = new Image(inputStream);
            newStage.getIcons().add(icon);
            newStage.show();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void close(ActionEvent event){
        Node source = (Node) event.getSource();
        Stage stage = (Stage) source.getScene().getWindow();
        stage.close();
    }
}