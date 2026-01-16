package sample;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class devScreen extends Application
{

    public void start(Stage primaryStage) throws Exception
    {

        Parent root = FXMLLoader.load(getClass().getResource("devScreen.fxml"));

        primaryStage.setTitle("Developer Login");

        primaryStage.setScene(new Scene(root, 300, 275));
        primaryStage.show();

    }
}