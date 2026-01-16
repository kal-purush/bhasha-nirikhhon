package com.dyspersja.mp3metadataeditor.view.editor;

import com.dyspersja.mp3metadataeditor.view.errorscreen.ErrorScreenProvider;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.AnchorPane;
import lombok.RequiredArgsConstructor;

import java.io.IOException;

@RequiredArgsConstructor
public class EditorInitializer {


    private final TitledPane ID3v2editorTitledPane;
    private final TitledPane ID3v1editorTitledPane;

    protected ID3v2ContentController loadID3v2Controller() {
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/fxml/ID3v2ContentPane.fxml"));
            AnchorPane pane = fxmlLoader.load();

            ID3v2ContentController id3v2ContentController = fxmlLoader.getController();

            ID3v2editorTitledPane.setContent(pane);
            return id3v2ContentController;
        } catch (IllegalStateException | IOException e) {
            ErrorScreenProvider.displayErrorWindow(e);
            return null;
        }
    }

    protected ID3v1ContentController loadID3v1Controller() {
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/fxml/ID3v1ContentPane.fxml"));
            AnchorPane pane = fxmlLoader.load();

            ID3v1ContentController id3v1ContentController = fxmlLoader.getController();

            ID3v1editorTitledPane.setContent(pane);
            return id3v1ContentController;
        } catch (IllegalStateException | IOException e) {
            ErrorScreenProvider.displayErrorWindow(e);
            return null;
        }
    }
}