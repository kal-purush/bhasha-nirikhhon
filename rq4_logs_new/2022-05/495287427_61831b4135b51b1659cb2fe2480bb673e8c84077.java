package mvc.model;

import java.sql.*;

import javax.servlet.http.HttpServlet;

public class DBConn extends HttpServlet{

   private static final long serialVersionUID = 1L;
   
   
   static public Connection dbconn() throws ClassNotFoundException, SQLException {
      Connection conn = null;
      try{
         
         String url = "jdbc:mysql://localhost:3306/jspProject?ServerTimezone=Asia/Seoul&useSSL=false";
         String user = "root";
         String password = "1234";
         
         Class.forName("com.mysql.jdbc.Driver");
         conn = DriverManager.getConnection(url, user, password);
         System.out.println("데이터베이스 연결을 성공했습니다. ");
         
      } catch (SQLException ex){
         
         System.out.println("데이터베이스 연결을 실패했습니다. <br>");
         System.out.println("SQLException: " + ex.getMessage());
      }
      return conn;
   }
}