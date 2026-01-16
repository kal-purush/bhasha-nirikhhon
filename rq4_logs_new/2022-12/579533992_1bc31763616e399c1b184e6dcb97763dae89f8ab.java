package com.jwy.exam.test;

import com.jwy.exam.Article;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCSelectTest {
  public static void main(String[] args){
    Connection con = null;
    PreparedStatement psts = null;
    ResultSet rs = null;
    List<Article> articles = new ArrayList<Article>();
    try{
      Class.forName("com.mysql.jdbc.Driver");
      String url = "jdbc:mysql://localhost:3306/text_board";
      con = DriverManager.getConnection(url, "jwy","1234");
      System.out.println("연결 성공 !");

      String sql = "SELECT *";
      sql += " FROM article";
      sql += " ORDER BY id DESC;";

      psts = con.prepareStatement(sql);
      rs = psts.executeQuery();

      while( rs.next() ){
        int id = rs.getInt("id");
        String regDate = rs.getString("regDate");
        String updateDate = rs.getString("updateDate");
        String title = rs.getString("title");
        String body = rs.getString("body");

        Article article = new Article(id, regDate, updateDate, title, body);
        articles.add(article);
      }
      System.out.println("결과 : " + articles);
    }catch(ClassNotFoundException e){
      System.out.println("연결 실패 !");
    }catch(SQLException e){
      System.out.println("에러 : " + e);
    }
    finally {
      try{
        if(con != null && !con.isClosed()){
          con.close();
        }
      }catch(SQLException e){
        e.printStackTrace();
      }
      try{
        if(psts != null && !psts.isClosed()){
          psts.close();
        }
      }catch(SQLException e){
        e.printStackTrace();
      }
    }
  }
}