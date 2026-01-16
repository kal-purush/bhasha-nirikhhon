package Entities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.swing.JLabel;
import javax.swing.table.DefaultTableModel;

import Fruit.Fruit;
import Main.Game;

public class PlayerParty {
    Connection con;
    Game gm;

    public PlayerParty(Game gm){
        loadDriver();
    }

    public void loadDriver(){
        //Creates a user specific database
        String url = "jdbc:mysql://localhost:3306/Game";
        String user = "root";
        String password = "Lu2nglu2.";
        try {
            con = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to databse");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveParty(String[] info){
        String save = "INSERT INTO party VALUES (";
        for(String i : info){
            save += "'" + i + "'" + ", ";
        }
        save += ")";

        try {
            Statement stmnt = con.createStatement();
            stmnt.execute(save);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public Fruit getFruit(){
        //FINISH THIS LATER
        String getName = "SELECT name FROM party WHERE currentHealth > 0 LIMIT 1";
        String name = "";
        try {
            Statement stmnt = con.createStatement();
            ResultSet result = stmnt.executeQuery(getName);
            name = result.getString(0);
            System.out.println(name);
            return gm.setFruit.getFruit(name);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        gm.setFruit.getFruit(name);

        return gm.fruits[0];
    }


}