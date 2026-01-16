package ru.online;

import ru.online.utility.Utility;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Modality;
import javafx.stage.Stage;

import java.io.IOException;
import java.io.InputStream;

public class AlertWindow {

    private static final String FXML = "/AlertScene.fxml";
    private static final String PATH_TO_CONFIRM_ICON = "/img/alert_icon.png";
    private static final String ALERT_TITLE = "Warning!";
    private static final int WINDOW_WIDTH = 200;
    private static final int WINDOW_HEIGHT = 80;

    public static void display() throws IOException {
        Stage confirmWindow = new Stage();
        confirmWindow.initModality(Modality.APPLICATION_MODAL);
        confirmWindow.setTitle(ALERT_TITLE);

        InputStream chatIconStream = ClientApp.class.getResourceAsStream(PATH_TO_CONFIRM_ICON);
        Image chatIcon = new Image(chatIconStream);
        confirmWindow.getIcons().add(chatIcon);

        FXMLLoader loader = new FXMLLoader(AlertWindow.class.getResource(FXML));
        Scene confirmScene = new Scene(loader.load(), WINDOW_WIDTH, WINDOW_HEIGHT);

        Utility.centerStage(confirmWindow, WINDOW_WIDTH, WINDOW_HEIGHT);
        confirmWindow.setScene(confirmScene);
        confirmWindow.setResizable(false);
        confirmWindow.showAndWait();
    }
}