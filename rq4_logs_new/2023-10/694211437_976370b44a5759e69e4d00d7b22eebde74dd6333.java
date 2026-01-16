package by.it_academy.team1.usermessages.dao.database;

import by.it_academy.team1.usermessages.core.entity.User;
import by.it_academy.team1.usermessages.dao.api.IUserDao;

import java.sql.*;
import java.util.Arrays;
import java.util.Map;

public class DatabaseUserDao implements IUserDao {

    public Boolean existsByUsername(String username) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/team_1")) {
            String savePositionSql = "SELECT exists (SELECT 1 FROM user_messages.users WHERE username = '?');";
            try (PreparedStatement preparedStatement = connection.prepareStatement(savePositionSql)) {
                preparedStatement.setString(1, username);
                preparedStatement.executeUpdate();
                ResultSet resultSet = preparedStatement.getResultSet();
                return resultSet.getBoolean(1);
            }
        } catch (SQLException exception) {
            System.out.println("SQL Exception " + exception.getErrorCode());
            System.out.println(Arrays.toString(exception.getStackTrace()));
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void saveNewUser(User user) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/team_1")) {
            String savePositionSql = "INSERT INTO user_messages.users" +
                    "(id, username, password, full_name, birthday, registration_date, role)" +
                    "VALUES (gen_random_uuid (),?,?,?,?,?,?);";
            try (PreparedStatement preparedStatement = connection.prepareStatement(savePositionSql)) {
                preparedStatement.setString(1, user.getUsername());
                preparedStatement.setString(2, user.getPassword());
                preparedStatement.setString(3, user.getFullName());
                preparedStatement.setString(4, user.getBirthday().toString());
                preparedStatement.setString(5, user.getRegisteredDate().toString());
                preparedStatement.setString(6, user.getRole().toString());
                preparedStatement.executeUpdate();
            }
        } catch (SQLException exception) {
            System.out.println("SQL Exception " + exception.getErrorCode());
            System.out.println(exception.getStackTrace().toString());
            throw new RuntimeException(exception);
        }
    }

    @Override
    public Map<String, User> getRegistrationUsers() { // TODO return List<user>
        return null;
    }

    @Override
    public User findUser(String username) {
        return null;
    }
}