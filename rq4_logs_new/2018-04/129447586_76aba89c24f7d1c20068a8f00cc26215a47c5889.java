package sample;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.net.URL;
import java.util.ResourceBundle;

public class EnterTask extends Application implements Initializable
{
    public static Stage primStage;
    EnterTask(String data)
    {
        String[] list= data.split(" ");
        
    }
    @Override
    public void start(Stage primaryStage) throws Exception
    {
        Parent root = FXMLLoader.load(getClass().getResource("sample.fxml"));
        primaryStage.setTitle("Enter Tasks");
        primaryStage.setScene(new Scene(root, 640, 480));
        primaryStage.show();
    }

    @Override
    public void initialize(URL location, ResourceBundle resources)
    {

    }
}