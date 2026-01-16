/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package DAOs;

import DB.DBConnection;
import Models.News;
import Models.NewsHistory;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Admin
 */
public class NewsHistoryDAO {

    Connection conn;
    PreparedStatement ps;
    ResultSet rs;

    public NewsHistoryDAO() {
        try {
            conn = DBConnection.connect();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(NewsHistoryDAO.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(NewsHistoryDAO.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public LinkedList<NewsHistory> getAll() {
        LinkedList<NewsHistory> list = new LinkedList<>();
        String sql = "Select * from news_history";
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                NewsHistory newsHistoty = new NewsHistory(rs.getInt("news_his_id"), rs.getInt("news_id"), rs.getInt("staff_id"),
                        rs.getString("action"), rs.getString("title"), rs.getString("image_url"), rs.getString("title_content"),
                        rs.getString("content1"), rs.getString("content2"), rs.getString("content3"), rs.getDate("create_date"), rs.getInt("isDelete"));
                list.add(newsHistoty);
            }
        } catch (SQLException e) {
            Logger.getLogger(NewsHistoryDAO.class.getName()).log(Level.SEVERE, null, e);
        }
        return list;
    }
}