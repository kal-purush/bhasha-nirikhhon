
package AccountInfo;

import login.LoginScreenController;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ResourceBundle;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.Label;
import javafx.scene.image.Image;
import javafx.scene.paint.ImagePattern;
import javafx.scene.text.Text;


public class AccountInfoController implements Initializable {
    @FXML
    private Text name;
    @FXML
    private Text balance;
    @FXML
     private Text account_no;
     @FXML
     private Text gender;
     @FXML
     private Text religion;
     @FXML
     private Text account_type;
     
     
    public void setInfo()
    {
         Connection con=null;
        PreparedStatement ps=null;
        ResultSet rs = null;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/bank","root","");
           String sql = "SELECT * FROM userdata WHERE AccountNo=?";
            ps = con.prepareStatement(sql);
            
            
            ps.setString(1, LoginScreenController.acc);
         
            rs=ps.executeQuery();

            
            
            if(rs.next())
            {
               String ann;
               account_no.setText(rs.getString("AccountNo"));
               gender.setText(rs.getString("Gender"));
               account_type.setText(rs.getString("AccountType"));
               religion.setText(rs.getString("Religion"));
               balance.setText("₹ "+rs.getString("Balance"));
               
               if(rs.getString("Gender").length()==4)
               {
                   ann="Mr. ";
                   
               }
               else if(rs.getString("MaritialStatus").length()==7)
               {
                   ann="Mrs. ";
               }
               else
               {
                   ann="Miss. ";
               }
               name.setText(ann+rs.getString("NAME"));
              
           
            }
            else
            {
             
            Alert a=new Alert(Alert.AlertType.ERROR);
            a.setTitle("Error");
            a.setHeaderText("Error in Login");
            a.setContentText("Your Account no or Pin is incorrect.");
            a.showAndWait();
           
            }
            
            
            
            
        }catch(Exception e)
        {
            Alert a=new Alert(Alert.AlertType.ERROR);
            a.setTitle("Error");
            a.setHeaderText("Error in creating Account.");
            a.setContentText("2Your Account is not created there is some technical issue."+ e.getMessage());
            a.showAndWait();
            
        }
        
    }
    @Override
    public void initialize(URL url, ResourceBundle rb) {
       setInfo();
    }    
    
}