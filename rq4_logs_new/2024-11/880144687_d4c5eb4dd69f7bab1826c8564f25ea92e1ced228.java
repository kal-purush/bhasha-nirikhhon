package org.example.wishlist2semester2024.repository;

import org.example.wishlist2semester2024.model.User;
import org.springframework.stereotype.Repository;

import java.sql.*;

@Repository
public class WishRepository {

    private String URL = "";
    private String USERNAME = "";
    private String PASSWORD = "";

    public void saveNewUser(User user){
        String sql = "INSERT INTO tablenavn (first_name, last_name, email, username, password) VALUES (?, ?, ?, ?, ?)";
        try (Connection con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        PreparedStatement stmt = con.prepareStatement(sql)) {
            stmt.setString(1, user.getFirst_name());
            stmt.setString(2, user.getLast_name());
            stmt.setString(3, user.getEmail());
            stmt.setString(4, user.getUsername());
            stmt.setString(5, user.getPassword());
            stmt.executeUpdate();

        } catch(SQLException e){
            throw new RuntimeException(e);
        }
    }
}