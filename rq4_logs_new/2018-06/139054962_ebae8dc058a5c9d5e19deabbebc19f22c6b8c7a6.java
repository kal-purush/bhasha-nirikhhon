package Connection;

import static java.lang.Class.forName;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;



public class connectionBancoDados {
    
    private static final String DRIVER   = "com.mysql.jdbc.Driver"; //carregando drive
    private static final String URL     = "jdbc:mysql://localhost:3307/conveniencia";
    private static final String USER    = "root";
    private static final String PASS    = "usbw";
    
    public static Connection getConnection(){
        try {
            forName(DRIVER);
            return DriverManager.getConnection(URL, USER, PASS);
        } catch (ClassNotFoundException | SQLException ex) {
           throw new RuntimeException("erro de conexão", ex);
        }
    }
    
    public static void closeConnetion(Connection con){
            try {
        if (con != null) {
                con.close();
        }        
            } catch (SQLException ex) {
                System.err.println("ERRO! "+ex);
            }
        }
    
    public static void closeConnetion(Connection con, PreparedStatement stmt){
        closeConnetion(con);
            try {
        if (stmt != null) {
                stmt.close();
        }        
            } catch (SQLException ex) {
                System.err.println("ERRO! "+ex);
            }
        }
   
    public static void closeConnetion(Connection con, PreparedStatement stmt, ResultSet rs){
        closeConnetion(con, stmt);
            try {
                if (rs != null) {
                    rs.close();
                }        
            } catch (SQLException ex) {
                System.err.println("ERRO! "+ex);
            }
        }

    public Object RST(String prod) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  
    
   
    
}