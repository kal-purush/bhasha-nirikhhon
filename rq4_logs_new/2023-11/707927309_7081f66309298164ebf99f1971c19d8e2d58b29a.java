package org.openjfx;

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.TilePane;
import javafx.scene.control.Label;
import javafx.stage.Stage;

public class App extends Application {

    @Override
    public void start(Stage s) {
        // set title for the stage
        s.setTitle("creating buttons");

        // create three buttons
        Button dataEntryButton = new Button("Data Entry");
        Button reviewerButton = new Button("Reviewer Button");
        Button approverButton = new Button("Approver Button");

        // create a stack pane
        TilePane r = new TilePane();

        // create a label
        Label l = new Label("Pick which page to go to :)");

        // action event
        EventHandler<ActionEvent> dataEntryClick = new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {

                DataEntryScene dataEntryScene = new DataEntryScene();
                Scene dataEntryScreen = dataEntryScene.createScene();
                s.setScene(dataEntryScreen);
                s.show();

            }
        };

        EventHandler<ActionEvent> reviewerClick = new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {

                ReviewerScene reviewerScene = new ReviewerScene();
                Scene reviewerScreen = reviewerScene.createScene();
                s.setScene(reviewerScreen);
                s.show();

            }
        };

        EventHandler<ActionEvent> approverClick = new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {

                ReviewerScene approverScene = new ReviewerScene();
                Scene approverScreen = approverScene.createScene();
                s.setScene(approverScreen);
                s.show();

            }
        };

        // when button is pressed
        dataEntryButton.setOnAction(dataEntryClick);
        reviewerButton.setOnAction(reviewerClick);
        approverButton.setOnAction(approverClick);

        // add button
        r.getChildren().add(l);
        r.getChildren().add(dataEntryButton);
        r.getChildren().add(reviewerButton);
        r.getChildren().add(approverButton);

        // create a scene
        Scene sc = new Scene(r, Constants.SCREEN_SIZE_X, Constants.SCREEN_SIZE_Y);

        // set the scene
        s.setScene(sc);

        s.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}