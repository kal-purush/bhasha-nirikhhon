package com.develogica.chapter_09;

import javafx.application.Application;
import javafx.event.EventHandler;
import javafx.event.EventType;
import javafx.scene.Scene;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.stage.Stage;

public class EventHandlerProperties extends Application {
    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage stage) throws Exception {
        var circle = new Circle(100, 100, 50);
        circle.setFill(Color.CORAL);

        var root = new HBox();
        root.getChildren().add(circle);

        var scene = new Scene(root);
        stage.setScene(scene);
        stage.setTitle("EventHandlerProperties Demo");
        stage.show();
        stage.sizeToScene();

        EventHandler<MouseEvent> eventFilter = event ->
                System.out.println("Mouse event filter has been called");

        EventHandler<MouseEvent> eventHandler = event ->
                System.out.println("Mouse event handler has been called");


        circle.addEventFilter(MouseEvent.MOUSE_CLICKED, eventFilter);

        circle.setOnMouseClicked(eventHandler);
    }
}