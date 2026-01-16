package com.dyspersja.mp3metadataeditor.errorscreen;

import javafx.application.Platform;
import javafx.scene.control.Alert;

public class ErrorScreenProvider {
    public static void displayErrorWindow(Exception e) {
        Alert alert = new Alert(Alert.AlertType.ERROR);

        alert.setTitle("Error");
        alert.setHeaderText("Error loading FXML");
        alert.setContentText("An error occurred while loading the FXML file: " + e.getMessage());

        alert.showAndWait();
        Platform.exit();
    }
}