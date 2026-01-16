package com.dyspersja.mp3metadataeditor.editor;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.TitledPane;
import javafx.stage.Stage;
import lombok.Setter;

import java.io.File;

public class EditorController {

    @FXML // fx:id="saveFileButton"
    private Button saveFileButton;

    @FXML // fx:id="ID3v2editorTitledPane"
    private TitledPane ID3v2editorTitledPane;
    @FXML // fx:id="createID3v2Button"
    private Button createID3v2Button;

    @FXML // fx:id="ID3v1editorTitledPane"
    private TitledPane ID3v1editorTitledPane;
    @FXML // fx:id="createID3v1Button"
    private Button createID3v1Button;

    @FXML
    @Setter
    private Stage window;

    @FXML
    void createID3v1(ActionEvent event) {

    }

    @FXML
    void createID3v2(ActionEvent event) {

    }

    @FXML
    void saveFile(ActionEvent event) {

    }

    public void setFile(File file) {

    }
}