package controllers;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.stage.Stage;

public class LauncherController {
    @FXML
    private Button Start;
    @FXML
    private Button Specifications;




    @FXML
    public void handleStartButton()
    {


    }

    @FXML
    public void handleSpecificationsButton()
    {
        Stage stage;
        Parent root;
        try{

            stage = (Stage) Specifications.getScene().getWindow();
            root = FXMLLoader.load(getClass().getClassLoader().getResource("Specifications.fxml"));
            Scene scene = new Scene(root);
            stage.setScene(scene);
            stage.show();

        } catch(Exception e) {
            e.printStackTrace();
            e.getCause();
        }

    }
}