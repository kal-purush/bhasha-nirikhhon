package server;

import java.sql.*;

public class DataBase {
    private static Connection connection;
    private static Statement statement;
    private static final String GET_NICKNAME_BY_LOG_PASS =
            "SELECT nickName FROM DataChat WHERE login=? and password=?";
    private static final String GET_LOG_PASS_NICKNAME = " select login, password, nickName from DataChat ";
    private static final String REG_INSERT = "INSERT INTO DataChat (login, password, nickName) VALUES (?,?,?)";
    private static final String CHANGE_NICK = "UPDATE DataChat SET nickName=? WHERE nickName=?;";
    private static final String CREATE_DB = "create table if not exists DataChat " +
            "(" + "id integer primary key autoincrement not null," +
            "login text not null," +
            "password text not null," + "" +
            "nickName" + ");";
    static PreparedStatement getLogPassNicknameStatement;
    static PreparedStatement regInsertStatement;
    static PreparedStatement getNicknameStatement;
    static PreparedStatement changeNickStatement;


    public DataBase() {
        try {
            connect();
            createTable();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createTable() throws SQLException {
        statement.executeUpdate(CREATE_DB);
    }


    public static void connect() throws SQLException {
        connection = DriverManager.getConnection("jdbc:sqlite:datadb.db");
        statement = connection.createStatement();
        System.out.println("connected to db");
        getLogPassNicknameStatement = connection.prepareStatement(GET_LOG_PASS_NICKNAME);
        regInsertStatement = connection.prepareStatement(REG_INSERT);
        getNicknameStatement = connection.prepareStatement(GET_NICKNAME_BY_LOG_PASS);
        changeNickStatement = connection.prepareStatement(CHANGE_NICK);


    }

    public void disconnect() {
        try {
            if (getNicknameStatement != null) getNicknameStatement.close();
            if (getLogPassNicknameStatement != null) getLogPassNicknameStatement.close();
            if (regInsertStatement != null) regInsertStatement.close();
            if (changeNickStatement != null) changeNickStatement.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
