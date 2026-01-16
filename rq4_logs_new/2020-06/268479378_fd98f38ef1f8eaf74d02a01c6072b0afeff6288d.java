package com.codecool.ccms.dao;

import com.codecool.ccms.models.Mentor;
import com.codecool.ccms.models.Role;
import com.codecool.ccms.models.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoImpl implements UserDao {

    protected Connection connection;
    protected Statement statement;

    public void connect() {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:src/main/resources/school");
            statement = connection.createStatement();
        } catch (ClassNotFoundException e) {
            e.getStackTrace();
        } catch (SQLException e) {
            System.out.println("Could not connect to DB " + e.getMessage());
        }
    }

    private void executeQuery(String  query){
        connect();
        try {
            statement.execute(query);
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<User> getUsers(String query) {
        List<User> users = new ArrayList<>();
        connect();
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String firstName = resultSet.getString("first_name");
                String surname = resultSet.getString("surname");
                String email = resultSet.getString("email");
                String password = resultSet.getString("password");
                Role role = Role.valueOf(resultSet.getInt("roleId"));
                //factory here
                User user = new Mentor(); //test
                users.add(user);
            }
            resultSet.close();
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return users;
    }

    @Override
    public void add(String table, String[] columns, String[] values) {
        String columnsAsQuery = String.join(",", columns);
        String valuesAsQuery = String.join(",", values);
        String query = String.format("INSERT INTO %s (%s) VALUES (%s);", table,
                columnsAsQuery, valuesAsQuery);
        executeQuery(query);

    }

    public void addUserToDB(String[] values) {
        String[] columns = {"name", "surname", "email", "password", "role"};
        for (int i = 0; i < columns.length; i++) {
            values[i] = String.format("'%s'", values[i]);
        }
        String table = "users";
        add(table, columns, values);
    }

    @Override
    public void remove(String table, String id) {
        String query = String.format("DELETE FROM %s WHERE Id = %s;", table, id);
        executeQuery(query);

    }

    protected void updateById(String table, String id, String column, String newValue) {
        String condition = String.format("id = %s", id);
        update(table, column, newValue, condition);
    }

    @Override
    public void update(String table, String column, String newValue, String condition) {
        if (column.toLowerCase().equals("id")) {
            System.out.println("Unable to change id");
            return;
        }
        String query = String.format("UPDATE %s SET %s = %s WHERE %s;", table, column, newValue, condition);
        executeQuery(query);
    }

    @Override
    public List<User> getAll() {
        return getUsers("SELECT * FROM users;");
    }
}