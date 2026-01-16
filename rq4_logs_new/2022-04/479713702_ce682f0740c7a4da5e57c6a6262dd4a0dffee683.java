
package dao;

import java.sql.Connection;
import java.sql.DriverManager;


public class DBConnection {
    private Connection connection;
     public Connection getConnection() {
        if (this.connection == null) {
            try {
                Class.forName("org.postgresql.Driver");
                this.connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/blog", "postgres", "12345");
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}