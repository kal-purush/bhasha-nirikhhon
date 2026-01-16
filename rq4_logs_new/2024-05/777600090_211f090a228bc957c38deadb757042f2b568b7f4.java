package homework_nr_22;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DB_Connection {
    private static Connection instance = null;

    private DB_Connection(){

    }
    public static Connection setNewConnection(String url, String user, String pass) throws SQLException {
        if(instance == null)
            return DriverManager.getConnection(url, user, pass);
        else return instance;
    }
}