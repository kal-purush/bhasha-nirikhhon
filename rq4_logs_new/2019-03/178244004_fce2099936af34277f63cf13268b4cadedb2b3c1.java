package app;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;

public class CreatePlayerDB {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
    static final String MYSQL_URL = "jdbc:mysql://localhost/";
    static final String APP_URL = "jdbc:mysql://localhost/APP";

    static final String USER = "root";
    static final String PASS = "root";

    static Connection connection;
    
    
    public static void main(String[] args) {
        try {
            createConnection(MYSQL_URL);
            createDatabase();

            createConnection(APP_URL);
            createTable(); 
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }

    private static void createConnection(String url) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(url, USER, PASS);
    }

    private static void createDatabase() throws SQLException {
        Statement stmt = connection.createStatement();            
        String sql = "CREATE DATABASE APP";
        stmt.executeUpdate(sql);
    }

    private static void createTable() throws SQLException {
        Statement stmt = connection.createStatement();            
        String sql = "CREATE TABLE PLAYER (nickname VARCHAR(36) not NULL, " +
        " PRIMARY KEY ( nickname ))";
        stmt.executeUpdate(sql);
    }
}