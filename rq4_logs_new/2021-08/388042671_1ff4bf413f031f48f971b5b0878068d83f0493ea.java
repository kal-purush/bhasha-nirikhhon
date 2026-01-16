package com.example.team5_final;

import android.util.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.SneakyThrows;

public class ConnToData {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    @SneakyThrows
    public String run(String mem_id){
        Connection conn = null;
        PreparedStatement pst = null;
        ResultSet rs = null;

        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection("jdbc:mysql://team05-data.co1wqvrxiuqf.us-east-2.rds.amazonaws.com","admin","11111111");

            String sql_getCnt = "SELECT loginCnt from member where mem_id = ?";
            pst = conn.prepareStatement(sql_getCnt);
            pst.setString(1, mem_id);
            pst.executeQuery();
        }catch(SQLException e){
            Log.d("sql error", "CONNECTION ERROR!");
        }finally {
            pst.close();
            conn.close();
        }

        return "";
    }
}