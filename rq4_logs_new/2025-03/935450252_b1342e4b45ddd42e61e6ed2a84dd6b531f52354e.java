package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.json.JSONArray;
import org.json.JSONObject;

public class Main {

    public static void PrintQueryResult(ResultSet rs)
    {
        try
        {
            int count = rs.getMetaData().getColumnCount();
            while(rs.next())
            {
                String out = "";
                for(int i = 1; i <= count ; i++)
                {
                    out += rs.getString(i);
                    out += "          ";
                }
                System.out.println(out);
            }
        }
        catch(Exception e)
        {
            System.out.println("Some error occured inside 'PrintQueryResult()' function");
            System.out.println(e);
        }
    }

    public static JSONArray ResultToJson(ResultSet rs)
    {   
        JSONArray arr = new JSONArray();
        try
        {
            int count = rs.getMetaData().getColumnCount();
            while(rs.next())
            {
                JSONObject obj = new JSONObject();
                for(int i = 1; i <= count ; i++)
                {
                    obj.put(rs.getMetaData().getColumnName(i), rs.getString(i));
                }
                arr.put(obj);   
            }
        }
        catch(Exception e)
        {
            System.out.println("Some error occured inside 'PrintQueryResult()' function");
            System.out.println(e);
        }
        return arr;
    }

    public static void main(String[] args) {
        
        try 
        {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/RecepieFinder", "zuhair", "hello");
        
            System.out.println("Connection to DB secured !!!");

            // -------------------------------------------------- End points to secure data from db

            // get all ingredients
            // PreparedStatement ps = connect.prepareStatement("SELECT * FROM Ingredient");

            // get all recepies
            // PreparedStatement ps = connect.prepareStatement("SELECT * FROM Recepie");

            // get all users
            // PreparedStatement ps = connect.prepareStatement("SELECT * FROM User");
            
            // get recepies that has a particular ingredient in it
            // PreparedStatement ps = connect.prepareStatement("select * from Recepie inner join RecepieIngredient using(recepieId) inner join Ingredient using(ingredientId) where ingredientName = 'cheese'");
            
            // get recepies that has a particular user has marked as favourite
            PreparedStatement ps = connect.prepareStatement("select * from Recepie inner join UserFavouriteRecepie using(recepieId) inner join User using(userId) where userEmail = 'zuhairhasan2003@gmail.com';");

            ResultSet rs = ps.executeQuery();

            JSONArray response = ResultToJson(rs);
            System.out.println(response);
        }
        catch(Exception e)
        {
            System.out.println("HAHAHA Error, get back to work !!!");
            System.out.println(e);
        }
    }
}