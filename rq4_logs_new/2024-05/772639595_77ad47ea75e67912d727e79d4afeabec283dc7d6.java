import java.sql.*;

public class DatabaseUtil {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/weather_db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "pa55word";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }

    public static void insertWeatherData(int locationId, String weatherJson) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO weather_data (location_id, weather_json) VALUES (?, ?)")) {
            stmt.setInt(1, locationId);
            stmt.setString(2, weatherJson);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Implement other database operations as needed
}