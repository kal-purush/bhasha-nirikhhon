package app;

import java.sql.*;

public class App {
    public static void main(String[] args) throws Exception {
        ResultSet users = DBManager.sendQuery(
            "SELECT * FROM `user`"
        );

        while (users.next()) {
            System.out.println("User #" + users.getString(1) + ": " + users.getString(2));
        }
    }
}