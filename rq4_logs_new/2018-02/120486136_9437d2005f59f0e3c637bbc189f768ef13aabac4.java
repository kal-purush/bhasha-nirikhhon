/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author lafa
 */
public class MyConnection {
    
    
   private String url="jdbc:mysql://localhost:3306/pidev";
   private String login="root";
   private String pwd="";
   private Connection con;
   private static MyConnection instance = null;

    private MyConnection() {
        
        try{
             con= DriverManager.getConnection(url, login, pwd);
             System.err.println("connexion ok!!");
           }
        catch(SQLException ex){
             System.err.println("connexion pas ok!!!!");
            }
    }
    
    public static MyConnection getInstance(){
    
        if (instance== null){
        instance =new  MyConnection();
        }
        return instance; 
    }

    public Connection getCon() {
        return con;
    }


}

