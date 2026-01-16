package nl.sean.dea;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    public static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306/spotitube?useSSL=false&serverTimezone=UTC";
    public static final String DB_USER = "root";
    public static final String DB_PASS = "spotitube";

    static {
        try {
            Class.forName(MYSQL_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        try {
            return DriverManager.getConnection(MYSQL_URL,
                    DB_USER,
                    DB_PASS);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}