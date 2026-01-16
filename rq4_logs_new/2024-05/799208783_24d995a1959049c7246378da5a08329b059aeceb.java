package util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnUtil {
    private static Connection connection;

    public static Connection getConnection() {
        if (connection == null) {
            try {
                // Load the MySQL JDBC driver
                Class.forName("com.mysql.cj.jdbc.Driver");

                // Connection details from DBPropertyUtil
                String url = "jdbc:mysql://localhost:3306/payrollmanagement"; // Change this to your DB URL
                String username = "root"; // Change this to your DB 
                String password = "Sadhana"; // Change this to your DB password

                // Establish connection
                connection = DriverManager.getConnection(url, username, password);
                System.out.println("Connection established: " + connection);
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

	public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
                System.out.println("Connection closed.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

	 
	 
 
 
}