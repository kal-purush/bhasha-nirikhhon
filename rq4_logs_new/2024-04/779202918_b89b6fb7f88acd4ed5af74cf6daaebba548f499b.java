package com.example.englishforkids.dao;

import com.example.englishforkids.model.Account;
import com.example.englishforkids.model.RememberLogin;
import com.example.englishforkids.myconnection.MySQLConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class RememberLoginDAO extends EngSysDAO<RememberLogin, String>{
    private static final String SELECT_ACCOUNT_BY_IP_QUERY = "SELECT * FROM REMEMBERACCOUNT WHERE IPAddress = ?";
    private static final String INSERT_REMEMBER_LOGIN_QUERY = "INSERT INTO REMEMBERACCOUNT(IdAccount, IPAddress) VALUE (?,?)";

    public void insert(RememberLogin entity){
        Connection connection = MySQLConnection.getConnection();
        if (connection != null) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(INSERT_REMEMBER_LOGIN_QUERY)) {
                preparedStatement.setString(1, entity.getIdAccount());
                preparedStatement.setString(2, entity.getIpAddress());
                int rowsAffected = preparedStatement.executeUpdate();
                if (rowsAffected > 0) {
                    System.out.println("Insertion successful.");
                } else {
                    System.out.println("Insertion failed.");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void update(RememberLogin entity){

    }

    public void delete(String id){

    }

    public RememberLogin selectById(String ipAddress){
        RememberLogin rememberLogin = null;
        Connection connection = MySQLConnection.getConnection();
        if (connection != null) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ACCOUNT_BY_IP_QUERY)) {
                preparedStatement.setString(1, ipAddress);
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    rememberLogin = new RememberLogin();
                    rememberLogin.setIdAccount(resultSet.getString("IdAccount"));
                    rememberLogin.setIpAddress(resultSet.getString("IPAddress"));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return rememberLogin;
    }

    public List<RememberLogin> selectAll(){
        return null;
    }

    protected List<RememberLogin> selectBySql(String sql, Object...args){
        return null;
    }

}