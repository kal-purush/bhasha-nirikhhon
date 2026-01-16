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
import javafx.scene.control.Label;
import javafx.stage.Stage;
import java.sql.*;

/**
 * FXML Controller class
 *
 * @author Maciek
 */
public class Moje_daneController implements Initializable {
    @FXML
    private Label l_imie;
    @FXML
    private Label l_nazwisko;
    @FXML
    private Label l_login;
    @FXML
    private Label l_haslo;
    @FXML
    private Label l_email;
    @FXML
    private Label l_telefon;
    @FXML
    private Button b_zmien_haslo;
    @FXML
    private Button b_popraw_dane;
    @FXML
    private Button b_wstecz;

    /**
     * Initializes the controller class.
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        
        
         try {
            loginController login = new loginController();
             System.out.println(login.ble);
Class.forName("com.mysql.jdbc.Driver");
Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/pz","root","");
PreparedStatement statment = con.prepareStatement("select Imie, Nazwisko, Mail, Telefon  from uzytkownicy where Mail='"+login.ble+"'");
ResultSet result = statment.executeQuery(); 

if(result.next()){
l_imie.setText(result.getString(1));
l_nazwisko.setText(result.getString(2));
l_email.setText(result.getString(3));
l_telefon.setText(result.getString(4));// do testowania
}
  
         
   } catch (ClassNotFoundException ex) {
            Logger.getLogger(Moje_daneController.class.getName()).log(Level.SEVERE, null, ex);
      } catch (SQLException ex) {
            Logger.getLogger(Moje_daneController.class.getName()).log(Level.SEVERE, null, ex);
        }     
        
        
         b_zmien_haslo.setOnAction(new EventHandler<ActionEvent>() {
    @Override
    public void handle(ActionEvent actionEvent) {
         
        try {
            
            Parent zmien_haslo_parent = FXMLLoader.load(getClass().getResource("zmien_haslo.fxml"));
            Scene zmien_haslo_scene = new Scene(zmien_haslo_parent);
            Stage zmien_haslo_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
            zmien_haslo_stage.setScene(zmien_haslo_scene);
            zmien_haslo_stage.show();
            
        } catch (IOException ex) {
            Logger.getLogger(mainController.class.getName()).log(Level.SEVERE, null, ex);
        }
 
    }
});
         
   b_popraw_dane.setOnAction(new EventHandler<ActionEvent>() {
    @Override
    public void handle(ActionEvent actionEvent) {
         
        try {
            
            Parent popraw_dane_parent = FXMLLoader.load(getClass().getResource("popraw_dane.fxml"));
            Scene popraw_dane_scene = new Scene(popraw_dane_parent);
            Stage popraw_dane_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
            popraw_dane_stage.setScene(popraw_dane_scene);
            popraw_dane_stage.show();
            
        } catch (IOException ex) {
            Logger.getLogger(mainController.class.getName()).log(Level.SEVERE, null, ex);
        }
 
    }
});        
  
  b_wstecz.setOnAction(new EventHandler<ActionEvent>() {
    @Override
    public void handle(ActionEvent actionEvent) {
         
        try {
                
                Parent main_parent = FXMLLoader.load(getClass().getResource("main.fxml"));
                Scene main_scene = new Scene(main_parent);
                Stage main_stage = (Stage) ((Node) actionEvent.getSource()).getScene().getWindow();
                main_stage.setScene(main_scene);
                main_stage.show();
                
            } catch (IOException ex) {
                Logger.getLogger(loginController.class.getName()).log(Level.SEVERE, null, ex);
            }
 
    }
});  
  
        
}}