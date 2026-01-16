package org.openjfx;

import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.stage.Stage;

public class BackButton {
    public Button createButton(Stage s) {

        // Return back
        Button backButton = new Button("Back");

        EventHandler<ActionEvent> back = new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {

                MainScene mainScene = new MainScene();
                Scene mainScreen = mainScene.createScene(s);
                s.setScene(mainScreen);
                s.show();

            }
        };
        backButton.setOnAction(back);

        return backButton;

    }
}