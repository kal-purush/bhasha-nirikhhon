package nl.sean.dea.dao;

import nl.sean.dea.ConnectionFactory;
import nl.sean.dea.dto.UserDTO;

import javax.enterprise.inject.Default;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Default
public class UserDAOImpl implements UserDAO {

    @Override
    public UserDTO getUser(String username, String password) {
        UserDTO foundUser = null;
        try (
                Connection connection = new ConnectionFactory().getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(
                        "SELECT * FROM user WHERE Username=? AND Password=?")
        ) {
            preparedStatement.setString(1, username);
            preparedStatement.setString(2, password);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                foundUser = new UserDTO(username, password);
            }
        } catch (SQLException e) {
            throw new RuntimeException();
        }
        return foundUser;
    }
}