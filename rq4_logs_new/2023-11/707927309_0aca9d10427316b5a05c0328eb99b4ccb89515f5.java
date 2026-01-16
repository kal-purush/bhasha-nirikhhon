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
import org.openjfx.App;

public class BackScene {

    public Scene createScene() {
        // set title for the stage
        
        App.s.setTitle("creating buttons");

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
                App.s.setScene(dataEntryScreen);
                App.s.show();
            }
        };

        EventHandler<ActionEvent> reviewerClick = new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {

                ReviewerScene reviewerScene = new ReviewerScene();
                Scene reviewerScreen = reviewerScene.createScene();
                App.s.setScene(reviewerScreen);
                App.s.show();

            }
        };

        EventHandler<ActionEvent> approverClick = new EventHandler<ActionEvent>() {
            public void handle(ActionEvent e) {

                ApproverScene approverScene = new ApproverScene();
                Scene approverScreen = approverScene.createScene();
                App.s.setScene(approverScreen);
                App.s.show();


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
        
        sc.getStylesheets().add(getClass().getResource("styles.css").toExternalForm());
        App.s.setScene(sc);

        App.s.show();
        App.s.setMaximized(true);
        
        return sc;

    }

}