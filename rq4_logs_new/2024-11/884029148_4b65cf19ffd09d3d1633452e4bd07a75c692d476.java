package data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCSQLite {
    private static final String dataPath = "jdbc:sqlite:data_sqlite.db";

    public static void main(String[] args) {

        try (Connection conn = DriverManager.getConnection(dataPath)) {
            if (conn != null) {
                System.out.println("Connected.");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }
}