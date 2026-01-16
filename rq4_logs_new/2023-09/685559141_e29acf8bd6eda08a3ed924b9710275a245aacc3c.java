package app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {

    private static  final String Driver_PATH = "com.mysql.cj.jdbc.Driver";
    private static final String DATABASE_URL = "jdbc:mysql://localhost:3303/mini_sas_db";
    private static final String USERNAME ="root";
    private static final String PASSWORD = "root";

    public Database(){
        try
        {
            Class.forName(Driver_PATH);
        } catch (Exception e)
        {
            throw new RuntimeException("SomeThing wrong with Database Constructor  : "+e);
        }

    }


    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
    }
}