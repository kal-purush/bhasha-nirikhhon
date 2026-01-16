package com.rizal;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;

public class App extends Application
{
    private BorderPane layout;
    public static void main( String[] args )
    {

        launch(args);
    }

    @Override
    public void start(Stage primaryStage) throws Exception
    {
        layout = new BorderPane();
        Scene scene = new Scene(layout, 800, 800);
        primaryStage.setScene(scene);
        primaryStage.setTitle("Ennol 1.0");
        primaryStage.show();


    }
}