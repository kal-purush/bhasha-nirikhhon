package lesson2.dbconn;

import java.sql.*;

public class JdbcExample {
//    private static final String QUERY_PASSWORD = "SELECT u.name, p.password FROM users u INNER JOIN passwords p on u.id = p.id;";
    private static final String URL = "jdbc:sqlite:database.db";

    public static void main(String[] args) {
        try {
            Class.forName("org.sqlite.JDBC");
            readFromDB("julia");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void readFromDB(String clientName) {
        try (Connection connection = DriverManager.getConnection(URL)) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT u.name, p.password FROM users u INNER JOIN passwords p on u.id = p.id WHERE u.name = ?");
            preparedStatement.setString(1, clientName);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                String password = resultSet.getString("password");
                System.out.println("name: " + name + ", password: " + password);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}