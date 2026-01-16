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
import javafx.scene.control.TextField;
import javafx.stage.Stage;

/**
 * FXML Controller class
 *
 * @author Maciek
 */
public class TokenController implements Initializable {
    @FXML
    private TextField f_klucz;
    @FXML
    private TextField f_nowe_haslo;
    @FXML
    private TextField f_powtorz_haslo;
    @FXML
    private Button b_anuluj;
    @FXML
    private Button b_ok;

    /**
     * Initializes the controller class.
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
       
         b_anuluj.setOnAction(new EventHandler<ActionEvent>() {
    @Override
    public void handle(ActionEvent actionEvent) {
         
        try {
            
            Parent login_parent = FXMLLoader.load(getClass().getResource("login.fxml"));
                Scene login_scene = new Scene(login_parent);
                Stage login_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
                login_stage.setScene(login_scene);
                login_stage.show();
            
        } catch (IOException ex) {
            Logger.getLogger(mainController.class.getName()).log(Level.SEVERE, null, ex);
        }
 
    }
});
  
 b_ok.setOnAction(new EventHandler<ActionEvent>() {
    @Override
    public void handle(ActionEvent actionEvent) {
         
        try {
            
            Parent login_parent = FXMLLoader.load(getClass().getResource("login.fxml"));
                Scene login_scene = new Scene(login_parent);
                Stage login_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
                login_stage.setScene(login_scene);
                login_stage.show();
            
        } catch (IOException ex) {
            Logger.getLogger(mainController.class.getName()).log(Level.SEVERE, null, ex);
        }
 
    }
});         
         
    }    
    
}