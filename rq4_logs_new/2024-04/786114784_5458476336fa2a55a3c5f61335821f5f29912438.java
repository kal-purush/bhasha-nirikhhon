package com.dyspersja.mp3metadataeditor;

import com.dyspersja.mp3metadataeditor.errorscreen.ErrorScreenProvider;
import com.dyspersja.mp3metadataeditor.filechooser.FileChooserController;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;

public class ApplicationLauncher extends Application {

    @Override
    public void start(Stage stage) {
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/fxml/FileChooser.fxml"));
            Scene scene = new Scene(fxmlLoader.load());

            FileChooserController controller = fxmlLoader.getController();
            controller.setWindow(stage);

            stage.setTitle("Choose a File");
            stage.setResizable(false);
            stage.setScene(scene);
            stage.show();
        } catch (IllegalStateException | IOException e) {
            ErrorScreenProvider.displayErrorWindow(e);
        }
    }

    public void launchApplication() {
        launch();
    }
}