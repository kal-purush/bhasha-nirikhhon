/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package projekt;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author Maciek
 */
public class Edytuj_uzytkownikaController implements Initializable {
    @FXML
    private Button b_zapisz;
    @FXML
    private Button b_wstecz;

    /**
     * Initializes the controller class.
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
          b_wstecz.setOnAction(new EventHandler<ActionEvent>() {
    @Override
    public void handle(ActionEvent actionEvent) {
         
        try {
                
                Parent uzytkownicy_parent = FXMLLoader.load(getClass().getResource("uzytkownicy.fxml"));
            Scene uzytkownicy_scene = new Scene(uzytkownicy_parent);
            Stage uzytkownicy_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
            uzytkownicy_stage.setScene(uzytkownicy_scene);
            uzytkownicy_stage.show();
                
            } catch (IOException ex) {
                Logger.getLogger(loginController.class.getName()).log(Level.SEVERE, null, ex);
            }
 
    }
});  
        
         b_zapisz.setOnAction(new EventHandler<ActionEvent>() {
    @Override
    public void handle(ActionEvent actionEvent) {
         
        try {
                
                Parent uzytkownicy_parent = FXMLLoader.load(getClass().getResource("uzytkownicy.fxml"));
            Scene uzytkownicy_scene = new Scene(uzytkownicy_parent);
            Stage uzytkownicy_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
            uzytkownicy_stage.setScene(uzytkownicy_scene);
            uzytkownicy_stage.show();
                
            } catch (IOException ex) {
                Logger.getLogger(loginController.class.getName()).log(Level.SEVERE, null, ex);
            }
 
    }
});  
    }    
    
}