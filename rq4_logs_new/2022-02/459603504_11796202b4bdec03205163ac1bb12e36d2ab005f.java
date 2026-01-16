package Data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnect {

    public Connection getConnection() {

        Connection conn = null;

        try {
            String url = "jdbc:sqlserver://DESKTOP-J8RPU6N:1433;DatabaseName=users;encrypt=true;trustServerCertificate=true;";
            String user = "newLogin";
            String password = "TestAutomation123";

            conn = DriverManager.getConnection(url,user,password);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }
}