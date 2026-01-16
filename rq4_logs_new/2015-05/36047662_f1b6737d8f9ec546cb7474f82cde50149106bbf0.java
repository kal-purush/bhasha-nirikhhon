package jp.ac.tsuda;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author 
 */
public class AccountDAO {
    public Account findByLogin(Login login) {
        Connection conn = null;
        Account account = null;
        try {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
            String driverURL = "jdbc:derby://localhost:1527/db2932";
            conn = DriverManager.getConnection(driverURL, "db2932", "db2932");
            
            String sql  = "SELECT NAME,PASS FROM DATA WHERE NAME=? AND PASS=?";
            PreparedStatement pStmt = conn.prepareStatement(sql);
            pStmt.setString(1,login.getName());
            pStmt.setString(2,login.getPass());
            
            ResultSet rs = pStmt.executeQuery();
            
            if(rs.next()) {
                String userName = rs.getString("NAME");
                String pass = rs.getString("PASS");
                
                account = new Account(userName,pass);
            }
        } catch(SQLException e) {
            e.printStackTrace();
            return null;
        } catch(ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } finally {
            if(conn!=null) {
                try {
                    conn.close();
                } catch(SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
        return account;
    }
}