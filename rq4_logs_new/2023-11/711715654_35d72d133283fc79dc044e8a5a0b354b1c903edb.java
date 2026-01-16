// Zohaib Ahmadzai
// CEN-3024C
// 11/12/2023


package com.example.lmsgui;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBC {
    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {

            Class.forName("org.sqlite.JDBC");


            String url = "jdbc:sqlite:C:\\sqlite\\sqlite.db";
            connection = DriverManager.getConnection(url);


            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT * FROM books");


            while (resultSet.next()) {
                System.out.println("Barcode: " + resultSet.getString("barcode"));
                System.out.println("Title: " + resultSet.getString("title"));
                System.out.println("Author: " + resultSet.getString("author"));

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (resultSet != null) resultSet.close();
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
