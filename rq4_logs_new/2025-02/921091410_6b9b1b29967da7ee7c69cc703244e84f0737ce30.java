package Assignment_4;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.swing.JTextArea;

public class AircraftDAO {
    public void addAircraft(String name, int maxPassengers, int airportId) {
        String sql = "INSERT INTO aircraft (name, max_passengers, airport_id) VALUES (?, ?, ?)";
        try (Connection connection = DatabaseConnection.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, name);
            stmt.setInt(2, maxPassengers);
            stmt.setInt(3, airportId);
            stmt.executeUpdate();
            System.out.println("Aircraft added: " + name);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteAircraft(int id) {
        String sql = "DELETE FROM aircraft WHERE id = ?";
        try (Connection connection = DatabaseConnection.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, id);
            stmt.executeUpdate();
            System.out.println("Aircraft deleted: ID " + id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void listAircraft(JTextArea displayArea) {
        String sql = "SELECT * FROM aircraft";
        try (Connection connection = DatabaseConnection.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql);
             var rs = stmt.executeQuery()) {
            while (rs.next()) {
                displayArea.append("Aircraft ID: " + rs.getInt("id") +
                        ", Name: " + rs.getString("name") +
                        ", Max Passengers: " + rs.getInt("max_passengers") +
                        ", Airport ID: " + rs.getInt("airport_id") + "\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}