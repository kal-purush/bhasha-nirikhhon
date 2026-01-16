package controller;

import java.sql.*;
import javax.swing.JOptionPane;

public class AdminMethods implements Repository{
    
    Connection connection = Database.getConnect();
    ResultSet result = null;
    private String sql;
    
    public AdminMethods() {
        //
    }

    @Override
    public boolean insert() {
        sql = "insert into admins values(?,?,?)";
        
        try {
            PreparedStatement insert = connection.prepareStatement(sql);
            
            insert.setInt(1, user.getID());
            insert.setString(2, user.getName());
            insert.setString(3, user.getPassword());
            insert.executeUpdate();
            return true;
            
        } catch(Exception ex) {
            JOptionPane.showMessageDialog(null, ex.getMessage(), "Error!!", JOptionPane.ERROR_MESSAGE);
        }
        return false;
    }

    @Override
    public void update() {

    }

    @Override
    public void delete() {

    }

    @Override
    public boolean check() {
        sql = "select admin_id, password from admins where admin_id = ? and password = ?";
        
        try {
            PreparedStatement check = connection.prepareStatement(sql);
            
            check.setInt(1, user.getID());
            check.setString(2, user.getPassword());
            result = check.executeQuery();
            
            if(result.next())
                return true;
            
        } catch(Exception ex) {
            JOptionPane.showMessageDialog(null, ex.getMessage(), "Error!1", JOptionPane.ERROR_MESSAGE);
        }
        return false;
    }

    @Override
    public String selectName() {
        return "";
    }

    @Override
    public boolean checkPassword() {
        return true;
    }
    
}