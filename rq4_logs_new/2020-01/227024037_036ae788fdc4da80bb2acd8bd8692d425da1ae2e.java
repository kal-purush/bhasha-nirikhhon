package com.tarnovskiy.server;

import java.sql.*;

/**
 * @author Tarnovskiy Maksim
 */
public class AuthService {
    private static Connection connection;
    private static Statement stmt;

    public static void connect() {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:mydb.db");
            stmt = connection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getnickByLoginAndPass(String login, String pass){
        String sql = String.format("SELECT nickname FROM main where login = '%s' and password = '%s'", login, pass);

        try {
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()){
                return rs.getString("nickname");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean searchClient(String nick){
        String nickSql = getNick(nick);
        boolean have;
        if (nickSql.equals(nick))
            have = true;
        else
            have = false;
        return have;
    }

    private static String getNick(String nick) {
        String sql = String.format("SELECT nickname FROM main where nickname = '%s'", nick);

        try {
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()){
                return rs.getString("nickname");
            } else return "";
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void disconnect(){
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}