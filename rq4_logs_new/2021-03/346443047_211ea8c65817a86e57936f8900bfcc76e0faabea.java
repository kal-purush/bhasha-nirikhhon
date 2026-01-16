package ru.online.auth;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SQLiteAuthService implements AuthService {

    private List<Client> clients;
    private Connection connection;
    private Statement statement;
    private PreparedStatement ps;

    public SQLiteAuthService() {
        clients = new ArrayList<>();
    }

    @Override
    public void start() {
        System.out.println("Connecting to DB.");
        connect();
        create();
        getClients();
        System.out.println("Connected to DB. Auth started.");
    }

    @Override
    public void stop() {
        System.out.println("Disconnecting from DB.");
        disconnect();
        System.out.println("Disconnected from DB. Auth stopped.");
    }

    @Override
    public String getUsernameByLoginPass(String login, String pass) {
        String username = "";
        try {
            ps = connection.prepareStatement("SELECT username FROM clients WHERE login = ? AND password = ?;");
            ps.setString(1, login);
            ps.setString(2, pass);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                username = rs.getString("username");
            }
        } catch (SQLException exception) {
            System.out.println(exception.getMessage());
        }
        return username ;
    }

    private void getClients() {
        try (ResultSet rs = statement.executeQuery("SELECT * FROM clients;")) {
            while (rs.next()) {
                clients.add(new Client(rs.getString("username"), rs.getString("login"), rs.getString("password")));
            }
        } catch (SQLException exception) {
            System.out.println(exception.getMessage());
        }
    }

    private void create() {
        try {
            statement.executeUpdate("DROP TABLE if EXISTS clients;");
            statement.executeUpdate("CREATE TABLE if NOT EXISTS clients (id INTEGER PRIMARY KEY autoincrement, username TEXT, login TEXT, password TEXT);");
            insertDefault();
        } catch (SQLException exception) {
            System.out.println(exception.getMessage());
        }
    }

    private void insertDefault() throws SQLException {
        statement.executeUpdate("INSERT INTO clients (username, login, password) VALUES ('user1', 'log1', 'pass1');");
        statement.executeUpdate("INSERT INTO clients (username, login, password) VALUES ('user2', 'log2', 'pass2');");
        statement.executeUpdate("INSERT INTO clients (username, login, password) VALUES ('user3', 'log3', 'pass3');");
    }

    private void connect() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException exception) {
            System.out.println(exception.getMessage());
        }
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:clients.db");
            statement = connection.createStatement();
        } catch (SQLException exception) {
            System.out.println(exception.getMessage());
        }
    }

    private void disconnect() {
        try {
            if (statement != null) statement.close();
        } catch (SQLException exception) {
            System.out.println(exception.getMessage());
        }
        try {
            if (connection != null) connection.close();
        } catch (SQLException exception) {
            System.out.println(exception.getMessage());
        }
    }
}