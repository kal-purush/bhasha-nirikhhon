/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controller;

import java.net.URL;
import java.util.ResourceBundle;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author Neusia
 */
public class Alert1Controller implements Initializable {

       @FXML
    private Button btnOK;

    @FXML
    private Label labelAlert;

    /**
     * Initializes the controller class.
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        // TODO
    }    
    
     
     public void fecharTela(){
          Stage stage =(Stage) btnOK.getScene().getWindow();
        stage.close();
      }
     
     

}