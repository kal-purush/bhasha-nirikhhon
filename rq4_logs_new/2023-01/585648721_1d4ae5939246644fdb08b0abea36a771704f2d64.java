package ru.iflex.springbootdemo.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtils {
    private static String dbURL = "jdbc:mysql://localhost:3307/springbootdemo";
    private static String dbUsername = "root";
    private static String dbPassword = "root";

    public static Connection getConnection(){
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(dbURL, dbUsername, dbPassword);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return connection;
    }
}