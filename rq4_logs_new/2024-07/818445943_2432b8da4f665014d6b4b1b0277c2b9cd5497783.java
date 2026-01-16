package ru.otus.chat.server.authbd;

import ru.otus.chat.server.AuthenticationProvider;
import ru.otus.chat.server.ClientPart;
import ru.otus.chat.server.Server;
import ru.otus.chat.server.UserRole;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class AuthUserJdbc implements AuthenticationProvider, AutoCloseable {
    private final Connection connection;
    private final Statement statement;
    private Server server;

    public AuthUserJdbc(Server server) throws SQLException {
        this.server = server;
        connection = DriverManager.getConnection(QueriesToBd.DATABASE_URL, "sazon", "sazon12345");
        statement = connection.createStatement();
        bdCreate();
    }

    private void bdCreate() throws SQLException {
        if (getAll().isEmpty()) {
            createUser(1, "admin", "user1", "pass1");
            createUser(2, "log2", "user2", "pass2");
            createUser(3, "log3", "user3", "pass3");
            createRole(1, UserRole.ADMIN);
            createRole(2, UserRole.USER);
            addRoleToUSer(1, 1);
            addRoleToUSer(2, 1);
            addRoleToUSer(2, 2);
            addRoleToUSer(2, 3);
        }
        System.out.println(getAll().toString());
        System.out.println();
    }

    @Override
    public List<User> getAll() {
        List<User> userLst = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(QueriesToBd.USERS_QUERY)) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String login = rs.getString("login");
                    String username = rs.getString("username");
                    String password = rs.getString("password");
                    User user = new User(id, login, username, password);
                    userLst.add(user);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (PreparedStatement ps = connection.prepareStatement(QueriesToBd.USER_ROLES_QUERY)) {
            for (User user : userLst) {
                ps.setInt(1, user.getId());
                List<Role> roleLst = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        int id = rs.getInt("id");
                        String name = rs.getString(2);
                        Role role = new Role(id, name);
                        roleLst.add(role);
                    }
                    user.setRoles(roleLst);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return userLst;
    }

    @Override
    public String getAdminName() {
        try (ResultSet rs = statement.executeQuery(QueriesToBd.ADMIN_NAME_QUERY)) {
            while (rs.next()) {
                return rs.getString("username");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean authenticate(ClientPart clientPart, String login, String password) {
        String authName = getUsernameByLogAndPass(login, password);
        if (authName == null) {
            clientPart.sendMessage("Некорректный логин/пароль");
            return false;
        }
        if (server.isUsernameIsBusy(authName)) {
            clientPart.sendMessage("Данное имя занято.");
            return false;
        }
        clientPart.setUsername(authName);
        server.subscribe(clientPart);
        clientPart.sendMessage("/correctauth " + authName);
        return true;
    }

    private String getUsernameByLogAndPass(String login, String password) {
        try (ResultSet rs = statement.executeQuery(QueriesToBd.LOGIN_USERNAME_PASSWORD_QUERY)) {
            while (rs.next()) {
                String log = rs.getString(1);
                String pass = rs.getString(3);

                if (log.equals(login) && pass.equals(password)) {
                    return rs.getString("username");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void createUser(int id, String login, String username, String password) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(QueriesToBd.CREATE_USER_QUERY);

        ps.setInt(1, id);
        ps.setString(2, login);
        ps.setString(3, username);
        ps.setString(4, password);
        ps.executeUpdate();

        System.out.println("В БД добавлен пользователь: " + login);
    }

    @Override
    public void createRole(int id, UserRole role) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(QueriesToBd.CREATE_ROLE_QUERY);

        ps.setInt(1, id);
        ps.setString(2, role.getValue());
        ps.executeUpdate();

        System.out.println("В БД добавлена роль: " + role.getValue());
    }

    @Override
    public void addRoleToUSer(int roleId, int userId) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(QueriesToBd.ADD_ROLE_TO_USER_QUERY);

        ps.setInt(1, roleId);
        ps.setInt(2, userId);
        ps.executeUpdate();

        System.out.println("Установлена роль для пользователя с id = " + userId);
    }

    @Override
    public void close() throws Exception {
        try {
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}