package com.dyspersja.mp3metadataeditor.view.errorscreen;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.stage.Stage;

public class IncorrectInputScreenController {

    @FXML // fx:id="okButton"
    private Button okButton;

    @FXML
    void closeWindow(ActionEvent event) {
        Stage stage = (Stage) okButton.getScene().getWindow();
        stage.close();
    }
}