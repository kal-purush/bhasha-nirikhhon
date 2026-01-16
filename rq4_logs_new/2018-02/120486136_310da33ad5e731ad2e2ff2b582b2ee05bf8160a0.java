/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sahtek;


import DB.MyConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.Pane;
import services.ServicesUser;

/**
 *
 * @author music
 */
public class FXMLDocumentController implements Initializable {
    Connection con ;
    @FXML
    private Label nouveau1;
    @FXML
    private Label status;
    @FXML
    private Pane rightContainer1;
    @FXML
    private Label nouveau;
    
     public FXMLDocumentController ()
    {
          con = MyConnection.getInstance().getCon() ;
    }
  
    
 
    

    @FXML
    private Pane rightContainer;
    @FXML
    private TextField userid;
    @FXML
    private PasswordField passwordid;
    @FXML
    private Button conn;
 
   
    @FXML
    public void Authentifier() 
              
        {
            
            ResultSet rs ;
        try {
            String query ="SELECT * from user where username=? and password=?" ; 
            PreparedStatement pref=con.prepareStatement(query);
            pref.setString(1, userid.getText());
            pref.setString(2, passwordid.getText());
           rs= pref.executeQuery() ;
           
           if (rs.next())
           {
               System.out.println("successfull");
           }
            
           else
           {
               System.out.println("not success");
           }
            
        } catch (SQLException ex) {
            Logger.getLogger(ServicesUser.class.getName()).log(Level.SEVERE, null, ex);
        }
            
            
        }
      
    
  
    
  
    
    


   
   
    
   
     
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        if (con==null)
            status.setText("non connecté") ;
        else
            status.setText(" connecté");
        
        // TODO
    }    

    @FXML
    private void z(MouseEvent event) {
    }
    
}