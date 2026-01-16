package controller;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.stage.Stage;

import java.awt.event.ActionEvent;
import java.net.URL;
import java.util.ResourceBundle;


public class Base implements Initializable {

    @FXML
    public static TextField driver;
    @FXML
    public static TextField url;
    @FXML
    public static TextField username;
    @FXML
    public static TextField password;
    @FXML
    public static Button btnSave;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        btnSave.setDisable(true);
    }
    @FXML
    public void btnSaveClick(){
        System.out.println("Zapisz dane do bazy");
        Stage stage = (Stage) btnSave.getScene().getWindow();
        stage.close();
    }

}