package DBconnection;



import java.awt.HeadlessException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import javax.swing.JOptionPane;



public class DBconnection {
    
private Connection connect = null;

   
    public Connection getConnection() {
        try{
            String url = "jdbc:mysql://10.30.56.254/product";
            String user = "root";
            String pass = "12345";
            String db = "product";
                        Class.forName("com.mysql.jdbc.Driver");
                        connect = DriverManager.getConnection(url, user, pass);
                        Statement stm = connect.createStatement();
            JOptionPane.showMessageDialog(null,"Connection Succes...");
        }catch(HeadlessException | ClassNotFoundException | SQLException e){
            JOptionPane.showMessageDialog(null,"Connecion Fail!!!");
        }return connect;
        }

public static void main (String[]args){
DBconnection obj = new DBconnection();
obj.getConnection();
    }  
}

   

