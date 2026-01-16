package JD;

import java.sql.*;

public class BoardDeleteExample {
    public static void main(String[] args) {

        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1/thisisjava?serverTimezone=UTC", "root", "0000");

            String sql = "delete from boards where bwriter=?";

            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "winter");

            int rows = pstmt.executeUpdate();   //  삭제 성공시 삭제 행 수 1 반환 : 0 반환
            System.out.println("삭제된 행 수: "+ rows);

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);

        } catch (SQLException e) {
            throw new RuntimeException(e);

        } finally {
            try {
                if (conn != null) conn.close();
                if (pstmt != null) pstmt.close();

            } catch (SQLException e) {
                throw new RuntimeException(e);

            }

        }
    }
}