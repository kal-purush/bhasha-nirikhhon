package com.mega.biz.sample.model;

import com.mega.config.database.JDBCUtilsHikariCP;
import com.mega.biz.sample.model.dto.SampleDTO;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SampleDAO2 {

    private Connection conn = null;
    private PreparedStatement pstmt = null;
    private ResultSet rs = null;

    public void insertUser(SampleDTO sampleDTO) {
//        Connection conn = null;
//        PreparedStatement pstmt = null;

        try {
            DataSource dataSource = JDBCUtilsHikariCP.getDataSource();
            conn = dataSource.getConnection();
            pstmt = conn.prepareStatement(SampleQuery.USER_INSERT.getQuery());

            pstmt.setString(1, sampleDTO.getId());
            pstmt.setString(2, sampleDTO.getPassword());
            pstmt.setString(3, sampleDTO.getName());
            pstmt.setString(4, sampleDTO.getRole());

            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // ResultSet이 없을 때 close
            JDBCUtilsHikariCP.close(conn, pstmt);
        }
    }

    public List<SampleDTO> getUserList() {
        List<SampleDTO> userList = new ArrayList<>();

        try {
            DataSource dataSource = JDBCUtilsHikariCP.getDataSource();
            conn = dataSource.getConnection();
            pstmt = conn.prepareStatement(SampleQuery.USER_LIST.getQuery());

            rs = pstmt.executeQuery();
            while (rs.next()) {
                SampleDTO user = new SampleDTO();
                user.setId(rs.getString("ID"));
                user.setPassword(rs.getString("PASSWORD"));
                user.setName(rs.getString("NAME"));
                user.setRole(rs.getString("ROLE"));
                userList.add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // ResultSet이 있을 때 close
            JDBCUtilsHikariCP.close(conn, pstmt, rs);
        }
        return userList;
    }
}